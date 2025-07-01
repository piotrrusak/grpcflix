from concurrent import futures

import grpc
import video_pb2
import video_pb2_grpc
import time
import threading

from collections import deque

class Server(video_pb2_grpc.VideoServiceServicer):

    def __init__(self, streamer_url='streamer:50002', source_id=0):
        self.streamer_url = streamer_url
        self.source_id = source_id
        self.queue = deque()
        self.stop = 0
        self.start = 1
        self.timestamp = 0
        threading.Thread(target=self.streamer_connection).start()

    def set_new_source(self, id):
        self.source_id = id
    
    def generator(self):
        if self.start:
            yield video_pb2.ClientMessage(
                start=video_pb2.StartRequest(timestamp=str(self.timestamp))
            )
            self.start = False

        while not self.stop:
            time.sleep(0.5)
        yield video_pb2.ClientMessage(
            stop=video_pb2.StopRequest(reason="server_cancelled")
        )
    
    def streamer_connection(self):
        print("streamer_connection")
        with grpc.insecure_channel(self.streamer_url) as channel:
            stub = video_pb2_grpc.VideoServiceStub(channel)

            response_stream = stub.Stream(self.generator())
            try:
                for chunk in response_stream:
                    self.queue.append(chunk.frames)
            except grpc.RpcError as e:
                print(f"RpcError: {e.code()} - {e.details()}.")

    def Stream(self, request_iterator, context):

        requests = set()
        
        def handle_requests():
            print(time.time())
            try:
                for msg in request_iterator:
                    if(msg.HasField("start")):
                        requests.add("start")
                    elif(msg.HasField("stop")):
                        requests.add("stop")
            except grpc.RpcError as e:
                print("Error: ", e)
        
        threading.Thread(target=handle_requests).start()

        while context.is_active():
            
            if len(self.queue) != 0:
                frames = self.queue.popleft()
                yield video_pb2.VideoChunk(frames=frames)
            else:
                time.sleep(0.01)

            if len(requests) != 0:
                requests.clear()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    video_pb2_grpc.add_VideoServiceServicer_to_server(Server(), server)
    server.add_insecure_port('0.0.0.0:50001')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()