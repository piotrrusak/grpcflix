from concurrent import futures

import grpc
import video_pb2
import video_pb2_grpc
import time
import threading
import cv2

from collections import deque

class Streamer(video_pb2_grpc.VideoServiceServicer):

    def __init__(self, source_id=0):
        self.source_id = source_id
        self.sources = dict()
        self.pointer = -1
        self.chunk_size = 1000
    
    def chunkify_photo(self):
        self.pointer += 1
        image = cv2.imread("image.jpeg")
        _, encoded_image = cv2.imencode('.jpg', image)
        return encoded_image.tobytes()[(self.pointer)*self.chunk_size:(self.pointer+1)*self.chunk_size]
    
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
            frames = self.chunkify_photo()
            if len(frames) == 0:
                break
            
            yield video_pb2.VideoChunk(frames=frames)

            if len(requests) != 0:
                print(requests)
                requests.clear()

def serve():
    streamer = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    video_pb2_grpc.add_VideoServiceServicer_to_server(Streamer(), streamer)
    streamer.add_insecure_port('0.0.0.0:50002')
    streamer.start()
    streamer.wait_for_termination()

if __name__ == '__main__':
    serve()