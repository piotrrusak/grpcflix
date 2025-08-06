import grpc
import client_server_pb2, client_server_pb2_grpc
import server_streamer_pb2, server_streamer_pb2_grpc

import sys, time, threading, collections, random, concurrent, os, yaml
import numpy as np

def connect_to_streamer(address, logger, retries=10, delay=2):
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

    def __init__(self, logger, streamer_url='localhost:50002', source_id=0):
        self.streamer_url = streamer_url
        self.logger = logger
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
        self.logger.info("streamer_connection")
        channel = connect_to_streamer(self.streamer_url, self.logger)
        stub = server_streamer_pb2_grpc.ServerStreamerServiceStub(channel)

        response_stream = stub.Stream(self.generator())
        try:
            for message in response_stream:
                if message.HasField("info"):
                    self.logger.info("Server got info.")
                    self.info = message.info.info
                elif message.HasField("chunk"):
                    self.logger.info("Server got chunk.")
                    self.queue.append(message.chunk.chunk)
        except grpc.RpcError as e:
            print(f"RpcError: {e.code()} - {e.details()}.")

    def Stream(self, request_iterator, context):

        def handle_requests():
            self.logger.info("handle_requests")
            try:
                for message in request_iterator:
                    if(message.HasField("client_start_request")):
                        self.logger.info("Server got start request from client.")
                    elif(message.HasField("client_stop_request")):
                        self.logger.info("Server got stop request from client.")
                    elif(message.HasField("client_pause_request")):
                        self.pause = 1
                        for key in self.client_status.keys():
                            self.client_status[key] = int(int(message.client_pause_request.timestamp) / 60)
                        print(self.client_status)
                        for key in self.outgoing.keys():
                            self.outgoing[key].append(("pause", int(message.client_pause_request.timestamp)))
                    elif(message.HasField("client_unpause_request")):
                        for key in self.client_status.keys():
                            self.client_status[key] = int(message.client_unpause_request.timestamp)//60
                        for key in self.outgoing.keys():
                            self.outgoing[key].append(("unpause", int(message.client_unpause_request.timestamp)//60))
                        self.pause = 0
                    elif(message.HasField("client_status_answer")):
                        for key in self.client_status.keys():
                            self.client_status[key] = int(int(message.client_status_answer.frame_id)/60)
                        for key in self.outgoing.keys():
                            self.outgoing[key].append(("pause", int(int(message.client_status_answer.frame_id))))
                        
            except grpc.RpcError as e:
                self.logger.error(f"RpcError: {e.code()} - {e.details()}")

        threading.Thread(target=handle_requests).start()

        if len(self.client_status) == 0:
            id = 0
            self.client_status[0] = 0
        else:
            id = random.randint(0, 10000)
            while id in self.client_status.keys():
                id = random.randint(0, 10000)
            self.client_status[id] = 0
            self.logger.info("NEW USER")
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
                self.logger.info(f"Send segment: {self.client_status[id]}")
                self.client_status[id] += 1
                yield client_server_pb2.ServerClientMessage(
                    chunk=client_server_pb2.ServerClientChunk(chunk=frames)
                )
            
            
            if id == 0 and self.new_user_pause:
                yield client_server_pb2.ServerClientMessage(
                    server_status_request=client_server_pb2.ServerStatusRequest()
                )
                self.new_user_pause = 0
            
            if len(self.outgoing[id]) > 0:
                typ, load = self.outgoing[id].popleft()

                if typ == "pause":
                    self.logger.info(f"send pause: {id}")
                    yield client_server_pb2.ServerClientMessage(
                        server_pause_request = client_server_pb2.ServerPauseRequest(timestamp=str(load))
                    )
                if typ == "unpause":
                    self.logger.info(f"send unpause: {id}")
                    yield client_server_pb2.ServerClientMessage(
                        server_unpause_request = client_server_pb2.ServerUnpauseRequest(timestamp=str(load))
                    )