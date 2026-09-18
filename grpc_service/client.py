import os
import grpc
import items_pb2
import items_pb2_grpc

# Fix 1: Single string for host:port
GRPC_TARGET = os.getenv('GRPC_TARGET', 'localhost:50052')


def new_items():
    """Generator for Client Streaming (AddItems).
    Yields individual CreateItemRequest messages one by one.
    """
    # Fix 2: Proper creation of individual Protobuf objects
    items_to_create = [
        items_pb2.CreateItemRequest(
            name="Cisco_CCIE Conference",
            date="Sep.30.2026 - Oct.10.2026",
            status="open",
            location="Berlin"
        ),
        items_pb2.CreateItemRequest(
            name="Arista_CSV Conference",
            date="Sep.30.2026 - Oct.10.2026",
            status="open",
            location="Frankfurt"
        )
    ]
    for item in items_to_create:
        print(f"Client sending: {item.name}")
        yield item


def chat_messages():
    """Generator for Bidirectional Streaming (ChatAbouttItem)."""
    # Fix 3: Passed integers for sequence, removed trailing commas
    messages = [
        items_pb2.ChatMessage(sender='client', message='Hello gRPC', sequence=1),
        items_pb2.ChatMessage(sender='client', message='Sending msg 2', sequence=2)
    ]
    for msg in messages:
        yield msg


def run():
    print(f'Connecting to {GRPC_TARGET}...')

    # Open the network channel to the server
    with grpc.insecure_channel(GRPC_TARGET) as channel:
        # Create the client stub (the object containing all network methods)
        stub = items_pb2_grpc.ItemServiceStub(channel)

        # -------------------------------------------------------------
        # 1. Unary RPC (1 Request -> 1 Response)
        # -------------------------------------------------------------
        print('\n--- 1. Unary RPC (GetItemById) ---')
        try:
            # Fix 4: Passed string id="event1" instead of int 1
            item = stub.GetItemById(items_pb2.ItemIdRequest(id="event10"))
            print(f"Response: {item.name} | {item.location} | {item.status}")
        except grpc.RpcError as e:
            print(f"Error: {e.details()}")

        # -------------------------------------------------------------
        # 2. Server Streaming RPC (1 Request -> Multiple Responses)
        # -------------------------------------------------------------
        print('\n--- 2. Server Streaming RPC (ListItems) ---')
        # Fix 5: Used singular ListItemRequest()
        response_stream = stub.ListItems(items_pb2.ListItemRequest())
        for item in response_stream:
            print(f"Streamed item: {item.id} - {item.name}")

        # -------------------------------------------------------------
        # 3. Client Streaming RPC (Multiple Requests -> 1 Response)
        # -------------------------------------------------------------
        print('\n--- 3. Client Streaming RPC (AddItems) ---')
        # Pass the generator function into the stub method
        result = stub.AddItems(new_items())
        print(f"Summary Response -> Created: {result.created_count}, Total in DB: {result.total_count}")

        # -------------------------------------------------------------
        # 4. Bidirectional Streaming RPC (Multiple Requests <-> Multiple Responses)
        # -------------------------------------------------------------
        print('\n--- 4. Bidirectional Streaming RPC (ChatAbouttItem) ---')
        # Pass generator into stub method and iterate over incoming responses immediately
        chat_responses = stub.ChatAbouttItem(chat_messages())
        for reply in chat_responses:
            print(f"Reply from {reply.sender}: '{reply.message}' (seq #{reply.sequence})")


if __name__ == '__main__':
    run()