import grpc
import client_server_pb2, client_server_pb2_grpc
import server_streamer_pb2, server_streamer_pb2_grpc

import sys, time, threading, collections, random, concurrent, os, yaml
import numpy as np

import logging

logger = logging.getLogger(__name__)

# logger.setLevel(logging.NOTSET / logging.DEBUG / logging.INFO / logging.WARNING / logging.ERROR / logging.CRITICAL)
# Sets the minimum level of logs that will be taken into account (i.e., processed).
# If not set, have value of logging.NOTSET, then logger takes value of root logger i.e. logging.WARNING.
logger.setLevel(logging.DEBUG)

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

class Servicer(client_server_pb2_grpc.ClientServerServiceServicer):

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
        self.info = ""
        self.source_id = source_id
        self.queue = collections.deque()
        self.stop = 0
        self.start = 1
        self.timestamp = 0
        self.pause = 0
        self.client_status = dict()
        self.outgoing = dict()
        time.sleep(3)
        self.new_user_pause = 0
        threading.Thread(target=self.streamer_connection).start()
        
    
    def generator(self):
        yield server_streamer_pb2.ServerStreamerMessage(
            server_start_request=server_streamer_pb2.ServerStartRequest()
        )
        while not self.stop:
            time.sleep(0.01)
        yield server_streamer_pb2.ServerStreamerMessage(
            server_stop_request=server_streamer_pb2.ServerStopRequest()
        )
    
    def streamer_connection(self):
        logger.info("streamer_connection")
        channel = connect_to_streamer(self.streamer_url)
        stub = server_streamer_pb2_grpc.ServerStreamerServiceStub(channel)

        response_stream = stub.Stream(self.generator())
        try:
            for message in response_stream:
                if message.HasField("info"):
                    logger.info("Server got info.")
                    self.info = message.info.info
                elif message.HasField("chunk"):
                    logger.info("Server got chunk.")
                    self.queue.append(message.chunk.chunk)
        except grpc.RpcError as e:
            print(f"RpcError: {e.code()} - {e.details()}.")

    def Stream(self, request_iterator, context):

        requests = dict()

        def handle_requests():
            logger.info("handle_requests")
            try:
                for msg in request_iterator:
                    if(msg.HasField("client_start_request")):
                        logger.info("Server got start request from client.")
                        requests["start"] = 0
                    elif(msg.HasField("client_stop_request")):
                        logger.info("Server got stop request from client.")
                        requests["stop"] = 0
                    elif(msg.HasField("client_pause_request")):
                        logger.info("Server got pause request from client.")
                        requests["pause"] = int(msg.client_pause_request.timestamp)
                    elif(msg.HasField("client_unpause_request")):
                        logger.info("Server got unpause request from client.")
                        requests["unpause"] = int(msg.client_unpause_request.timestamp)
            except grpc.RpcError as e:
                logger.error(f"RpcError: {e.code()} - {e.details()}")

        
        threading.Thread(target=handle_requests).start()

        if len(self.client_status) == 0:
            id = 0
            self.client_status[0] = 0
        else:
            id = random.randint(0, 10000)
            while id in self.client_status.keys():
                id = random.randint(0, 10000)
            self.client_status[id] = 0
            logger.info("NEW USER")
            self.new_user_pause = 1
        self.outgoing[id] = collections.deque()
        
        yield client_server_pb2.ServerClientMessage(
            info=client_server_pb2.ServerClientInfo(info=self.info)
        )

        while context.is_active():
            yield client_server_pb2.ServerClientMessage(
                heartbeat=client_server_pb2.ServerClientHeartbeat()
            )

            if not self.pause and self.client_status[id] < len(self.queue) and len(self.outgoing[id]) == 0:
                frames = self.queue[self.client_status[id]]
                self.client_status[id] += 1
                logger.info(f"Send segment: {self.client_status[id]}")
                yield client_server_pb2.ServerClientMessage(
                    chunk=client_server_pb2.ServerClientChunk(chunk=frames)
                )
            
            if id == 0 and self.new_user_pause:
                yield client_server_pb2.ServerClientMessage(
                    server_new_user_joined_request=client_server_pb2.ServerNewUserJoinedRequest()
                )
                self.new_user_pause = 0
            
            if len(self.outgoing[id]) > 0:
                typ, load = self.outgoing[id].popleft()

                if typ == "pause":
                    logger.info(f"send pause: {id}")
                    yield client_server_pb2.ServerClientMessage(
                        server_pause_request = client_server_pb2.ServerPauseRequest(timestamp=str(load))
                    )
                if typ == "unpause":
                    logger.info(f"send unpause: {id}")
                    yield client_server_pb2.ServerClientMessage(
                        server_unpause_request = client_server_pb2.ServerUnpauseRequest(timestamp=str(load))
                    )

            if len(requests) != 0:
                if "start" in requests.keys():
                    pass
                if "stop" in requests.keys():
                    pass
                if "pause" in requests.keys():
                    self.pause = 1
                    for key in self.client_status.keys():
                        self.client_status[key] = int(int(requests["pause"]) / 60)
                    print(self.client_status)
                    for key in self.outgoing.keys():
                        self.outgoing[key].append(("pause", requests["pause"]))
                    requests.pop("pause")
                if "unpause" in requests.keys():
                    for key in self.client_status.keys():
                        self.client_status[key] = int(requests["unpause"]/60)
                    for key in self.outgoing.keys():
                        self.outgoing[key].append(("unpause", int(requests["unpause"]/60)))
                    requests.pop("unpause")
                    self.pause = 0

 
def serve():
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    client_server_pb2_grpc.add_ClientServerServiceServicer_to_server(Servicer(), server)
    server.add_insecure_port('0.0.0.0:50001')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()