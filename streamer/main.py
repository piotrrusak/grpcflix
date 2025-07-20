import grpc
import video_pb2
import video_pb2_grpc

import sys, time, threading, collections, random, concurrent, utils, os, yaml
import numpy as np

import logging

logger = logging.getLogger(__name__)

# logger.setLevel(logging.NOTSET / logging.DEBUG / logging.INFO / logging.WARNING / logging.ERROR / logging.CRITICAL)
# Sets the minimum level of logs that will be taken into account (i.e., processed).
# If not set, have value of logging.NOTSET, then logger takes value of root logger i.e. logging.WARNING.
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler("client.log", mode='w')
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Because of root logger (every custom logger is child of root logger)
logger.propagate = False

with open(os.path.join(os.path.dirname(__file__), 'config.yml'), 'r') as f:
    config = yaml.safe_load(f)

class Streamer(video_pb2_grpc.VideoServiceServicer):

    def __init__(self, source_id=0):
        self.source_id = source_id
        self.sources = dict()
        self.pointer = -1
        self.chunk_size = 2097152 # 2MB
        self.current_data = utils.convert_video_to_bytes("video.mp4")
    
    def get_next_chunk_of_current_data(self):
        self.pointer += 1
        return self.current_data[(self.pointer)*self.chunk_size:(self.pointer+1)*self.chunk_size]
    
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
            frames = self.get_next_chunk_of_current_data()
            if len(frames) == 0:
                break
            
            yield video_pb2.VideoChunk(frames=frames)

            if len(requests) != 0:
                print(requests)
                requests.clear()

def serve():
    streamer = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    video_pb2_grpc.add_VideoServiceServicer_to_server(Streamer(), streamer)
    streamer.add_insecure_port('0.0.0.0:50002')
    streamer.start()
    streamer.wait_for_termination()

if __name__ == '__main__':
    serve()