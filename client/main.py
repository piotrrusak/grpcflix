import grpc
import video_pb2
import video_pb2_grpc

import sys, time, threading, collections, cv2, multiprocessing, yaml, os
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

def connect_to_server(address, retries=10, delay=2):
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

class Client:

    def __init__(
                 self,
                 server_url_docker='server:50001',
                 server_url='localhost:50001',
                 ):
        self.start = 1
        self.timestamp = 0
        self.stop = 0
        self.queue = collections.deque()

        if config["docker"] == "True":
            self.server_url = server_url_docker
        else:
            self.server_url = server_url

        time.sleep(4)

    def generator(self):
        if self.start:
            yield video_pb2.ClientMessage(
                start=video_pb2.StartRequest(timestamp=str(self.timestamp))
            )
            self.start = False

        while not self.stop:
            time.sleep(0.5)
        yield video_pb2.ClientMessage(
            stop=video_pb2.StopRequest(reason="user_cancelled")
        )
    
    def server_connection(self):
        logger.info("server_connection")
        channel = connect_to_server(self.server_url)
        stub = video_pb2_grpc.VideoServiceStub(channel)
        response_stream = stub.Stream(self.generator())
        buffer = b''
        try:
            for chunk in response_stream:
                buffer += chunk.frames

                while len(buffer) >= (1080 * 1080 * 3):
                    frame_bytes = buffer[:1080 * 1080 * 3]
                    buffer = buffer[1080 * 1080 * 3:]
                    
                    frame = np.frombuffer(frame_bytes, dtype=np.uint8).reshape((1080, 1080, 3))
                    cv2.imshow("Klatka", frame)
                    if cv2.waitKey(int(1000 / 60)) & 0xFF == ord('q'):
                        self.stop = True
                        break

                if self.stop:
                    break

        except grpc.RpcError as e:
            print(f"RpcError: {e.code()} - {e.details()}.")


if __name__ == '__main__':
    client = Client()

    # threading.Thread(target=client.server_connection).start()
    multiprocessing.Process(target=client.server_connection()).start()

    time.sleep(5)
    client.stop = True