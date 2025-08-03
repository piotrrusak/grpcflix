import grpc
import server_streamer_pb2, server_streamer_pb2_grpc

import sys, time, threading, collections, random, concurrent, utils, os, yaml, cv2, av, json
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

class Streamer(server_streamer_pb2_grpc.ServerStreamerServiceServicer):

    def __init__(self, source_id=0):
        self.source_id = source_id
        self.sources = dict()
        self.pointer = -1
        # self.chunk_size = 2097152 # 2MB
        self.chunk_size = 3499200 # 1080 * 1080 * 3
        # self.chunk_size = 3145728 # 3MB
        # self.chunk_size =  4194304 # 4MB
        self.current_data = b''
        self.info = ""
        self.load_data("output/")
        
    def get_next_chunk_of_current_data(self):
        self.pointer += 1
        logger.info("1: " + str(len(self.current_data)) + "\n2: " + str(self.pointer*self.chunk_size))
        return self.current_data[(self.pointer)*self.chunk_size:(self.pointer+1)*self.chunk_size]
    
    def load_data(self, input_dir_path):
        self.current_data = b''
        self.info = ""
        for filename in sorted(os.listdir(input_dir_path)):
            print(filename)
            if filename == "info.json":
                continue
            with open(os.path.join(input_dir_path, filename), 'rb') as f:
                self.current_data += f.read()
        with open(os.path.join(input_dir_path, "info.json"), 'r') as f:
            self.info += f.read()

    def Stream(self, request_iterator, context):
        requests = set()
        def handle_requests():
            print(time.time())
            try:
                for message in request_iterator:
                    if(message.HasField("server_start_request")):
                        requests.add("start")
                    elif(message.HasField("server_stop_request")):
                        requests.add("stop")
            except grpc.RpcError as e:
                print("Error: ", e)
        
        threading.Thread(target=handle_requests).start()

        yield server_streamer_pb2.StreamerServerMessage(
            info=server_streamer_pb2.StreamerServerInfo(info=self.info)
        )

        while context.is_active():
            chunk = self.get_next_chunk_of_current_data()
            if len(chunk) == 0:
                break
            logger.info(str(f"Send video chunk ({self.pointer}) to server, timestamp: " + str(time.time())))
            yield server_streamer_pb2.StreamerServerMessage(
                chunk=server_streamer_pb2.StreamerServerChunk(chunk=chunk)
            )

            if len(requests) != 0:
                print(requests)
                requests.clear()

def serve():
    streamer = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    server_streamer_pb2_grpc.add_ServerStreamerServiceServicer_to_server(Streamer(), streamer)
    streamer.add_insecure_port('0.0.0.0:50002')
    streamer.start()
    streamer.wait_for_termination()

if __name__ == '__main__':
    serve()