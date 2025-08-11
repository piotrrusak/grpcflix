import grpc
import client_server_pb2, client_server_pb2_grpc
import server_streamer_pb2, server_streamer_pb2_grpc

import sys, time, threading, collections, random, concurrent, os, yaml, json
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

class Server(client_server_pb2_grpc.ClientServerServiceServicer):

    def __init__(self, logger, streamer_url='localhost:50002'):
        self.logger = logger
        self.streamer_url = streamer_url
        
        self.info_str = ""
        self.info = None
        self.queue = collections.deque()
        
        self.server_servicer_status = dict()
        self.outgoing = dict()
        self.source = None
        self.upload_queue = collections.deque()
        self.upload_filename = None

        self.event_flag = {
            "server_have_to_ask_initial_client_for_status": False,
            "client_wants_source": False,
            "client_finished_upload": False
        }
        
        self.status_flag = {
            "pause": False,
            "upload_in_progress": False
        }

        threading.Thread(target=self.streamer_connection).start()
        
    
    def generator(self):
        
        yield server_streamer_pb2.ServerStreamerMessage(
            server_start_request=server_streamer_pb2.ServerStartRequest()
        )

        while True:
            
            if self.event_flag["client_wants_source"]:
                self.info_str = ""
                self.info = None
                self.queue = collections.deque()
                self.logger.info(f"Server sends server_source_request to streamer.")
                yield server_streamer_pb2.ServerStreamerMessage(
                    server_source_request = server_streamer_pb2.ServerSourceRequest(source=self.source)
                )
                for key in self.server_servicer_status.keys():
                    self.server_servicer_status[key]["segment_id"] = 0
                for key in self.outgoing.keys():
                    self.outgoing[key].append(("info_resend", ""))
                self.event_flag["client_wants_source"] = False
                for key in self.outgoing.keys():
                    self.outgoing[key].append(("pause", 0))
            
            if self.event_flag["client_finished_upload"]:
                self.logger.info("Server start uploading to streamer.")
                yield server_streamer_pb2.ServerStreamerMessage(
                    server_upload_start = server_streamer_pb2.ServerUploadStart()
                )
                while len(self.upload_queue) > 0:
                    yield server_streamer_pb2.ServerStreamerMessage(
                        server_upload_chunk = server_streamer_pb2.ServerUploadChunk(chunk = self.upload_queue.popleft())
                    )
                yield server_streamer_pb2.ServerStreamerMessage(
                    server_upload_end = server_streamer_pb2.ServerUploadEnd(filename = self.upload_filename)
                )
                self.logger.info("Server end uploading to streamer.")
                self.event_flag["client_finished_upload"] = False
            
            time.sleep(0.01)
    
    def streamer_connection(self):
        self.logger.info("streamer_connection")
        channel = connect_to_streamer(self.streamer_url, self.logger)
        stub = server_streamer_pb2_grpc.ServerStreamerServiceStub(channel)

        response_stream = stub.Stream(self.generator())
        try:
            for message in response_stream:
                
                if message.HasField("info"):
                    self.logger.info("Server got info.")
                    self.info_str = message.info.info
                    self.info = json.loads(self.info_str)
                
                elif message.HasField("chunk"):
                    self.logger.debug("Server got chunk.")
                    self.queue.append(message.chunk.chunk)
                
                elif message.HasField("streamer_no_such_file_request"):
                    self.logger.info(f"Streamer has no requested file")
                    # self.outgoing[0].append(("choose_source", ""))
                
                elif message.HasField("heartbeat"):
                    # self.logger.debug("Server got heartbeat from streamer")
                    pass
                
        except grpc.RpcError as e:
            self.logger.error(f"RpcError.")

    def Stream(self, request_iterator, context):

        if 0 not in self.server_servicer_status.keys():
            id = 0
            self.logger.info(f"First user joined to server.")
        else:
            id = random.randint(0, 10000)
            while id in self.server_servicer_status.keys():
                id = random.randint(0, 10000)
            self.logger.info(f"New user joined to server.")
        
        self.server_servicer_status[id] = {
            "status": "initialised", # initialised | info
            "segment_id": 0
        }
        self.event_flag["server_have_to_ask_initial_client_for_status"] = True
        self.status_flag["pause"] = True

        self.outgoing[id] = collections.deque()
        self.outgoing[id].append(("info_resend", ""))

        def handle_requests():
            self.logger.info("Server: Thread handle_requests starts")
            try:
                for message in request_iterator:

                    if(message.HasField("client_start_request")):
                        self.logger.info(f"Server got start request from client: {id}.")
                    
                    elif(message.HasField("client_stop_request")):
                        self.logger.info(f"Server got stop request from client with id: {id}.")
                        del self.server_servicer_status[id]
                        self.info_str = ""
                        
                        if len(self.server_servicer_status) == 0:
                            self.queue.clear()
                            self.event_flag["client_wants_source"] = False
                        
                        return

                    elif(message.HasField("client_pause_request")):
                        self.status_flag["pause"] = True
                        for key in self.server_servicer_status.keys():
                            self.server_servicer_status[key]["segment_id"] = int(message.client_pause_request.frame_id)//int(round(self.info[-1][2]))
                        for key in self.outgoing.keys():
                            self.outgoing[key].append(("pause", int(message.client_pause_request.frame_id)))
                    
                    elif(message.HasField("client_unpause_request")):
                        for key in self.server_servicer_status.keys():
                            self.server_servicer_status[key]["segment_id"] = int(message.client_unpause_request.frame_id)//int(round(self.info[-1][2]))
                        for key in self.outgoing.keys():
                            self.outgoing[key].append(("unpause", int(message.client_unpause_request.frame_id)))
                        self.status_flag["pause"] = False
                    
                    elif(message.HasField("client_status_answer")):
                        for key in self.server_servicer_status.keys():
                            self.server_servicer_status[key]["segment_id"] = int(message.client_status_answer.frame_id)//int(round(self.info[-1][2]))
                        for key in self.outgoing.keys():
                            self.outgoing[key].append(("pause", int(int(message.client_status_answer.frame_id))))
                    
                    elif(message.HasField("client_choose_source_answer")):
                        self.logger.info(f"Server got client_choose_source_answer: {message.client_choose_source_answer.source}")
                        self.source = message.client_choose_source_answer.source
                        self.event_flag["client_wants_source"] = True
                    
                    # UPLOAD START
                    
                    elif(message.HasField("client_upload_start")):
                        self.logger.info(f"Server got client_upload_start.")
                        self.status_flag["upload_in_progress"] = True
                    
                    elif(message.HasField("client_upload_chunk")):
                        self.logger.debug(f"Server got client_upload_chunk.")
                        self.upload_queue.append(message.client_upload_chunk.chunk)
                    
                    elif(message.HasField("client_upload_end")):
                        self.logger.info(f"Server got client_upload_end.")
                        self.upload_filename = message.client_upload_end.filename
                        self.status_flag["upload_in_progress"] = False
                        self.event_flag["client_finished_upload"] = True
                    
                    # UPLOAD END
                        
            except grpc.RpcError as e:
                self.logger.error(f"RpcError (probably disconnected)")
                del self.server_servicer_status[id]
                
                if len(self.server_servicer_status) == 0:
                    self.queue.clear()
                    self.info_str = ""
                    self.info = None
                    self.event_flag["client_wants_source"] = False

                return

        threading.Thread(target=handle_requests).start()

        while context.is_active():
            
            if id == 0 and self.event_flag["server_have_to_ask_initial_client_for_status"] and not self.server_servicer_status[id]["status"] == "initialised":
                self.logger.info(f"Server send server_status_request to client with id: 0")
                yield client_server_pb2.ServerClientMessage(
                    server_status_request=client_server_pb2.ServerStatusRequest()
                )
                self.event_flag["server_have_to_ask_initial_client_for_status"] = False

            elif not self.status_flag["pause"] and self.server_servicer_status[id]["segment_id"] < len(self.queue) and len(self.outgoing[id]) == 0:
                self.logger.debug(f"Server send segment: {self.server_servicer_status[id]["segment_id"]}")
                frames = self.queue[self.server_servicer_status[id]["segment_id"]]
                self.server_servicer_status[id]["segment_id"] += 1
                yield client_server_pb2.ServerClientMessage(
                    chunk=client_server_pb2.ServerClientChunk(chunk=frames)
                )
            
            else:
                # self.logger.debug(f"Server sends heartbeat to client: {id}")
                yield client_server_pb2.ServerClientMessage(
                    heartbeat=client_server_pb2.ServerClientHeartbeat()
                )
            
            # OUTGOING PROCESSING START
            
            if len(self.outgoing[id]) > 0:
                typ, load = self.outgoing[id].popleft()

                if typ == "pause":
                    self.logger.info(f"Server send pause request to client: {id}")
                    yield client_server_pb2.ServerClientMessage(
                        server_pause_request = client_server_pb2.ServerPauseRequest(frame_id=str(load))
                    )
                if typ == "unpause":
                    self.logger.info(f"Server send unpause request to client: {id}")
                    yield client_server_pb2.ServerClientMessage(
                        server_unpause_request = client_server_pb2.ServerUnpauseRequest(frame_id=str(load))
                    )
                if typ == "info_resend":
                    if self.info_str != "":
                        self.logger.info(f"Server resends info to client: {id}")
                        yield client_server_pb2.ServerClientMessage(
                            info=client_server_pb2.ServerClientInfo(info=self.info_str)
                        )
                        self.server_servicer_status[id]["status"] = "info"
                    else:
                        self.outgoing[id].append(("info_resend", ""))
            
            # OUTGOING PROCESSING END