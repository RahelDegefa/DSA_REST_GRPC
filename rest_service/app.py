from flask import Flask, jsonify, request
from waitress import serve
import os
import grpc
from pybreaker import CircuitBreakerError
import items_pb2
import items_pb2_grpc
from reliability import BackendUnavailable, protected_call

app = Flask(__name__)

GRPC_TARGET = os.getenv("GRPC_TARGET", "grpc-service:50051")
channel = grpc.insecure_channel(GRPC_TARGET)
stub = items_pb2_grpc.ItemServiceStub(channel)

app.json.compact = False

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"}), 200


def handle_backend_error(e):
    """
    Helper function to inspect BackendUnavailable or RpcError exceptions
    and differentiate between Database failure and gRPC service failure.
    """
    # Check if the root cause stored inside the exception is a gRPC RpcError
    cause = getattr(e, '__cause__', e)

    if isinstance(cause, grpc.RpcError):
        # FIX: ALREADY_EXISTS (e.g. a duplicate item id) used to fall through
        # to the generic "backend_failure" 502 below, which is misleading —
        # the gRPC service and Mongo are both working fine, the write was
        # just rejected as a conflict. Now reported as its own 409.
        if cause.code() == grpc.StatusCode.ALREADY_EXISTS:
            return jsonify({
                "error": "conflict",
                "message": cause.details() or "Item already exists."
            }), 409

        details = (cause.details() or "").lower()
        # If server.py explicitly threw FAILED_PRECONDITION or sent a DB message
        if cause.code() == grpc.StatusCode.FAILED_PRECONDITION or "mongodb" in details or "database" in details:
            return jsonify({
                "error": "database_failure",
                "message": "gRPC service is online, but MongoDB is unreachable."
            }), 503

        # FIX 4 (safety net): if a call ever times out before the server can
        # return its specific error (should be rare now that the client
        # timeout has headroom over the server's Mongo timeout — see
        # server.py and the ListItems/GetItemById/AddItems calls above),
        # report it honestly as a timeout instead of silently folding it
        # into the generic "backend_failure" case below.
        if cause.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            return jsonify({
                "error": "backend_timeout",
                "message": "gRPC service did not respond in time."
            }), 504

    # Default fallback for actual gRPC server crash / unreachable backend
    return jsonify({
        "error": "backend_failure",
        "message": "gRPC service did not respond successfully."
    }), 502


# -------------------------------------------------------------------
# GET /items
# -------------------------------------------------------------------
@app.route('/items', methods=['GET'])
def get_events():
    def grpc_list_operation():
        # FIX 4: was timeout=2.0, same as server's old serverSelectionTimeoutMS
        # — see the comment on MongoClient() in server.py for why that race
        # mattered. 3.5s gives the server's 1s Mongo-detection (plus retry
        # overhead in call_with_retries) comfortable room to finish and
        # return its specific error before the client gives up.
        stream_response = stub.ListItems(items_pb2.ListItemRequest(), timeout=3.5)
        items_list = []
        for item in stream_response:
            items_list.append({
                "id": item.id,
                "name": item.name,
                "status": item.status,
                "location": item.location,
                "date": item.date
            })
        return items_list

    try:
        items = protected_call(grpc_list_operation)
        return jsonify(items), 200
    except CircuitBreakerError:
        return jsonify({"error": "backend_unavailable", "message": "gRPC service is temporarily unavailable."}), 503
    except (BackendUnavailable, grpc.RpcError) as e:
        return handle_backend_error(e)


# -------------------------------------------------------------------
# GET /items/<item_id>
# -------------------------------------------------------------------
@app.route('/items/<string:item_id>', methods=['GET'])
def get_event(item_id):
    def grpc_get_by_id():
        request_msg = items_pb2.ItemIdRequest(id=str(item_id))
        # FIX 4: see comment on the ListItems call above.
        item = stub.GetItemById(request_msg, timeout=3.5)
        return {
            "id": item.id,
            "name": item.name,
            "status": item.status,
            "location": item.location,
            "date": item.date
        }

    try:
        item = protected_call(grpc_get_by_id)
        return jsonify(item), 200
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            return jsonify({"error": "Event not found"}), 404
        return handle_backend_error(e)
    except CircuitBreakerError:
        return jsonify({"error": "backend_unavailable", "message": "gRPC service is temporarily unavailable."}), 503
    except BackendUnavailable as e:
        return handle_backend_error(e)


# -------------------------------------------------------------------
# POST /items
# -------------------------------------------------------------------
@app.route('/items', methods=['POST'])
def create_event():
    data = request.get_json() or {}

    if 'name' not in data or 'location' not in data:
        return jsonify({"error": "Missing required fields: 'name' and 'location'"}), 400

    def grpc_operation():
        request_message = items_pb2.CreateItemRequest(
            name=data["name"].strip(),
            status=data.get("status", "open").strip(),
            location=data["location"].strip(),
            date=data.get("date", "2026-08-22").strip()
        )
        # FIX 4: see comment on the ListItems call above.
        return stub.AddItems(iter([request_message]), timeout=3.5)

    try:
        result = protected_call(grpc_operation)
        return jsonify({
            "message": "created through gRPC",
            "created_count": result.created_count,
            "total_count": result.total_count
        }), 201
    except CircuitBreakerError:
        return jsonify({"error": "backend_unavailable", "message": "gRPC service is temporarily unavailable."}), 503
    except (BackendUnavailable, grpc.RpcError) as e:
        return handle_backend_error(e)


if __name__ == '__main__':
    serve(app, host='0.0.0.0', port=5000)