import grpc
import video_pb2
import video_pb2_grpc

import sys, time, threading, collections, random, concurrent, os, yaml
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

def connect_to_streamer(address, retries=10, delay=2):
    for attempt in range(retries):
        try:
            logger.info(f"Connecting to streamer at {address} (attempt {attempt + 1})")
            channel = grpc.insecure_channel(address)
            grpc.channel_ready_future(channel).result(timeout=3)
            logger.info("Successfully connected to streamer!")
            return channel
        except grpc.FutureTimeoutError:
            logger.warning(f"Streamer not ready yet, retrying in {delay} seconds...")
            time.sleep(delay)
    raise ConnectionError(f"Failed to connect to streamer at {address} after {retries} attempts.")

class Servicer(video_pb2_grpc.VideoServiceServicer):

    def __init__(
                 self, 
                 streamer_url_docker='streamer:50002',
                 streamer_url='localhost:50002',
                 source_id=0
                 ):
        if config["docker"] == "True":
            self.streamer_url = streamer_url_docker
        else:
            self.streamer_url = streamer_url
        self.source_id = source_id
        self.queue = collections.deque()
        self.stop = 0
        self.start = 1
        self.timestamp = 0
        self.client_status = dict()
        time.sleep(3)
        threading.Thread(target=self.streamer_connection).start()
        
    
    def generator(self):
        if self.start:
            yield video_pb2.ClientMessage(
                start=video_pb2.StartRequest(timestamp=str(self.timestamp))
            )
            self.start = False

        while not self.stop:
            time.sleep(0.01)
        yield video_pb2.ClientMessage(
            stop=video_pb2.StopRequest(reason="server_cancelled")
        )
    
    def streamer_connection(self):
        logger.info("streamer_connection")
        channel = connect_to_streamer(self.streamer_url)
        stub = video_pb2_grpc.VideoServiceStub(channel)

        response_stream = stub.Stream(self.generator())
        try:
            for chunk in response_stream:
                self.queue.append(chunk.frames)
        except grpc.RpcError as e:
            print(f"RpcError: {e.code()} - {e.details()}.")

    def Stream(self, request_iterator, context):

        requests = set()

        id = random.randint(0, 10000)
        while id in self.client_status.keys():
            id = random.randint(0, 10000)

        self.client_status[id] = 0
        
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
            logger.debug(f"streamer_got_chunk {time.time()}")
            frames = self.queue[self.client_status[id]]
            self.client_status[id] += 1
            yield video_pb2.VideoChunk(frames=frames)

            if len(requests) != 0:
                requests.clear()

def serve():
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    video_pb2_grpc.add_VideoServiceServicer_to_server(Servicer(), server)
    server.add_insecure_port('0.0.0.0:50001')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()