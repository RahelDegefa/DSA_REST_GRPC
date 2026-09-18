import logging
import os
from concurrent import futures
import grpc
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
import items_pb2
import items_pb2_grpc
from pymongo.errors import (
    PyMongoError,
    ServerSelectionTimeoutError,
    DuplicateKeyError,
)
import time

logging.basicConfig(level=logging.INFO)

SERVICE_NAME = os.getenv("SERVICE_NAME", "items-grpc-service")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "itemsdb")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "items")

# FIX 4: was serverSelectionTimeoutMS=2000, the exact same value as the
# client's per-call gRPC timeout (timeout=2.0 in app.py). That meant the
# server's "Mongo is unreachable" detection and the client's deadline were
# racing each other — if the client's deadline fired first, it only saw a
# generic DEADLINE_EXCEEDED instead of the server's specific
# FAILED_PRECONDITION abort, so "Mongo down" and "grpc down" were
# indistinguishable to the REST layer. Lowered to 1000ms so the server always
# finishes detecting the outage with headroom before the client gives up.
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1000)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]
# FIX 2 (support): backing collection for the atomic id counter used by next_id()
counters = db["counters"]

try:
    collection.create_index([("id", ASCENDING)], unique=True)
except Exception:
    pass

# FIX 1: check_mongo_connection now retries with backoff instead of a single
# best-effort ping. Previously, if Mongo wasn't reachable in that one instant
# (e.g. right after a container restart, before Mongo had fully accepted
# connections), this just logged a WARNING and let serve() continue anyway —
# so the service reported itself "listening" while every real DB call would
# fail downstream. That's what produced "gRPC service is online, but MongoDB
# is unreachable" in the logs.
def check_mongo_connection(max_attempts=5, base_delay=1.0):
    for attempt in range(1, max_attempts + 1):
        try:
            client.admin.command("ping")
            logging.info("Connected to MongoDB at %s", MONGO_URI)
            return
        except (ServerSelectionTimeoutError, PyMongoError) as e:
            logging.warning(
                "MongoDB ping attempt %s/%s failed: %s", attempt, max_attempts, e
            )
            if attempt == max_attempts:
                raise
            time.sleep(base_delay * attempt)  # simple linear backoff

# FIX 2: next_numeric_id() used to do a non-atomic read-then-increment
# (find max id, add 1, insert). Under concurrent AddItems calls this is a
# race condition: two requests can read the same max id and both try to
# insert the same new id, and the second insert hits the unique index and
# raises DuplicateKeyError. Because DuplicateKeyError is a subclass of
# PyMongoError, it was being caught by the old blanket
# "except (ServerSelectionTimeoutError, PyMongoError)" below and misreported
# as "MongoDB is unreachable" even though Mongo was working fine.
# find_one_and_update with $inc is atomic at the database level, so this
# race can no longer happen.
def next_id():
    counter = counters.find_one_and_update(
        {"_id": "item_id"},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return str(counter["seq"])

def document_to_item(doc):
    return items_pb2.Item(
        id=str(doc["id"]),
        name=doc["name"],
        status=doc["status"],
        date=doc["date"],
        location=doc["location"]
    )

def validate_create_request(request):
    if not request.name.strip():
        return "Field 'name' is required."
    if not request.status.strip():
        return "Field 'status' is required."
    if not request.date.strip():
        return "Field 'date' is required."
    if not request.location.strip():
        return "Field 'location' is required."
    return None

class ItemService(items_pb2_grpc.ItemServiceServicer):

    def GetItemById(self, request, context):
        logging.info("GetItemById id=%s", request.id)
        try:
            doc = collection.find_one({"id": str(request.id)}, {"_id": False})
            if doc is None:
                context.abort(grpc.StatusCode.NOT_FOUND, f"Item {request.id} not found")
            return document_to_item(doc)
        # FIX 3: previously "except (ServerSelectionTimeoutError, PyMongoError)"
        # caught every pymongo error under one message. Now only genuine
        # connectivity failures are reported as "unreachable"; anything else
        # is logged with its real exception and reported as an internal error
        # instead of being mislabeled.
        except ServerSelectionTimeoutError:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Database connection failed: MongoDB is unreachable.")
        except PyMongoError as e:
            logging.exception("Unexpected MongoDB error in GetItemById")
            context.abort(grpc.StatusCode.INTERNAL, f"Database operation failed: {e}")

    def ListItems(self, request, context):
        logging.info("ListItems")
        try:
            documents = collection.find({}, {"_id": False}).max_time_ms(1000)
            for doc in documents:
                yield document_to_item(doc)
        # FIX 3 (same as GetItemById): distinguish real connectivity failures
        # from other Mongo errors instead of flattening them together.
        except ServerSelectionTimeoutError:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Database connection failed: MongoDB is unreachable.")
        except PyMongoError as e:
            logging.exception("Unexpected MongoDB error in ListItems")
            context.abort(grpc.StatusCode.INTERNAL, f"Database operation failed: {e}")

    def AddItems(self, request_iterator, context):
        logging.info("AddItems stream started")
        created_count = 0
        try:
            for request in request_iterator:
                problem = validate_create_request(request)
                if problem is not None:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, problem)

                # FIX 2: was next_numeric_id() (non-atomic read+increment,
                # see comment above its old definition). Now uses the atomic
                # counter-based next_id() so concurrent AddItems calls can't
                # compute the same id and collide on insert.
                new_id = next_id()
                document = {
                    "id": new_id,
                    "name": request.name.strip(),
                    "date": request.date.strip(),
                    "status": request.status.strip(),
                    "location": request.location.strip()
                }
                collection.insert_one(document)
                created_count += 1

            total_count = collection.count_documents({})
            return items_pb2.AddItemResult(created_count=created_count, total_count=total_count)
        # FIX 3: same split as the other methods, plus a dedicated branch for
        # DuplicateKeyError (still possible in principle, e.g. if a document
        # were ever inserted with a manually-set id) so it's reported as
        # ALREADY_EXISTS rather than as "MongoDB is unreachable".
        except ServerSelectionTimeoutError:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Database connection failed: MongoDB is unreachable.")
        except DuplicateKeyError:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, "Item with this id already exists.")
        except PyMongoError as e:
            logging.exception("Unexpected MongoDB error in AddItems")
            context.abort(grpc.StatusCode.INTERNAL, f"Database operation failed: {e}")

    def ChatAbouttItem(self, request_iterator, context):
        for message in request_iterator:
            yield items_pb2.ChatMessage(
                sender=SERVICE_NAME,
                message=f"received message {message.sequence}: '{message.message}'",
                sequence=message.sequence
            )

def serve():
    # FIX 1 (continued): if all retries in check_mongo_connection() are
    # exhausted, that's a real startup failure — let it crash the process
    # instead of logging a warning and starting anyway. Docker's healthcheck/
    # restart policy is what should retry a genuinely bad startup, not a
    # server that silently reports itself ready while unable to reach Mongo.
    check_mongo_connection()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    items_pb2_grpc.add_ItemServiceServicer_to_server(ItemService(), server)
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    logging.info("%s listening on port %s", SERVICE_NAME, GRPC_PORT)
    server.wait_for_termination()

if __name__ == "__main__":
    serve()