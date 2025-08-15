import grpc
import server_streamer_pb2, server_streamer_pb2_grpc
from video_segmenter import VideoSegmenter

import sys, time, threading, collections, random, concurrent, os, yaml, cv2, json
import numpy as np

class Streamer(server_streamer_pb2_grpc.ServerStreamerServiceServicer):

    def __init__(self, logger):
        self.logger = logger
        
        self.sources = dict()
        
        self.streamer_servicer_status = dict()
        self.streamer_servicer_data = dict()
        self.outgoing = dict()

        self.active_loaders = dict()

    def load_data(self, id, input_dir_path):
        self.logger.info(f"Streamer starts load_data: {input_dir_path}")
        while self.streamer_servicer_status[id]["status"] != "initialised":
            time.sleep(0.01)
        with open(os.path.join(input_dir_path, "info.json"), 'r') as f:
            self.streamer_servicer_data[id][1] = f.read()
        with open(os.path.join(input_dir_path, "info.json"), 'r') as f:
            self.streamer_servicer_data[id][2] = json.load(f)
        time.sleep(0.1)
        for filename in sorted(os.listdir(input_dir_path)):
            if filename == "info.json":
                continue
            if self.active_loaders[id] == 1:
                with open(os.path.join(input_dir_path, filename), 'rb') as f:
                    self.streamer_servicer_data[id][0].append(f.read())
            else:
                break
            time.sleep(0.1)
        self.active_loaders[id] -= 1
        self.logger.info(f"Streamer ends load_data: {input_dir_path}")

    def Stream(self, request_iterator, context):
        
        if len(self.streamer_servicer_status) == 0:
            id = 0
            self.logger.info(f"First server joined to streamer.")
        else:
            id = random.randint(0, 10000)
            while id in self.streamer_servicer_status.keys():
                id = random.randint(0, 10000)
            self.logger.info(f"New server joined to streamer.")
        
        self.streamer_servicer_status[id] = {
            "segment_id": 0,
            "status": "initialised"
        }
        self.streamer_servicer_data[id] = [collections.deque(), None, None]
        self.outgoing[id] = collections.deque()
        self.active_loaders[id] = 0

        streamer_servicer_source = {
            "sample.mp4": True
        }

        def source_updater(self, filename):
            while True:
                if os.path.exists(os.path.join("segment", filename.split(".")[0], "info.json")):
                    streamer_servicer_source[filename] = True
                    self.outgoing[id].append(("new_source", filename))
                    break
                else:
                    self.logger.info(f"STREAMER - {id} - source_updater - segmentation lasts")
                    time.sleep(5)

        def handle_requests(self):
            try:
                for message in request_iterator:
                    
                    if(message.HasField("server_streamer_source")):
                        if not os.path.exists(f"resource/{message.server_streamer_source.source}"):
                            self.logger.warning(f"No such source as: {message.server_streamer_source.source}")
                        
                        self.active_loaders[id] += 1
                        while self.active_loaders[id] != 1:
                            time.sleep(0.01)
                        
                        self.streamer_servicer_data[id][0] = collections.deque()
                        self.streamer_servicer_data[id][1] = None
                        self.streamer_servicer_data[id][2] = None
                        self.streamer_servicer_status[id]["status"] = "initialised"
                        self.streamer_servicer_status[id]["segment_id"] = 0
                        self.outgoing[id].append(("resend_info", ""))
                        threading.Thread(target=self.load_data, args=(id, "segment/" + message.server_streamer_source.source.split(".")[0])).start()

                    # UPLOAD START

                    elif(message.HasField("server_streamer_upload_start")):
                        upload_buffer = b''
                    
                    elif(message.HasField("server_streamer_upload_chunk")):
                        upload_buffer += message.server_streamer_upload_chunk.chunk
                    
                    elif(message.HasField("server_streamer_upload_end")):
                        with open(f"resource/{message.server_streamer_upload_end.filename}", 'wb') as f:
                            self.logger.info(f"Write in file: {message.server_streamer_upload_end.filename}")
                            f.write(upload_buffer)
                            del upload_buffer
                        threading.Thread(target = source_updater, args = (self, message.server_streamer_upload_end.filename)).start()
                        if not os.path.exists(f"segment/{message.server_streamer_upload_end.filename.split(".")[0]}/info.json"):
                            streamer_servicer_source[message.server_streamer_upload_end.filename] = False
                            self.logger.info(f"Not found segments of: {message.server_streamer_upload_end.filename.split(".")[0]}. Segmentation starts.")
                            threading.Thread(target=VideoSegmenter(f"resource/{message.server_streamer_upload_end.filename}", f"segment/{message.server_streamer_upload_end.filename.split(".")[0]}/info.json", f"segment/{message.server_streamer_upload_end.filename.split(".")[0]}", 1).segment).start()
                    
                    # UPLOAD END 
            
            except grpc.RpcError as e:
                self.logger.error(f"RpcError")
        
        threading.Thread(target=handle_requests, args=(self,)).start()

        while context.is_active():

            if self.streamer_servicer_data[id][2] != None and self.streamer_servicer_status[id]["segment_id"] < len(self.streamer_servicer_data[id][2]) and len(self.outgoing[id]) == 0 and len(self.streamer_servicer_data[id][0]) > 0:
                segment = self.streamer_servicer_data[id][0].popleft()
                self.logger.debug(str(f"Send segment no.: {self.streamer_servicer_status[id]["segment_id"]} to server."))
                yield server_streamer_pb2.StreamerServerMessage(
                    streamer_server_segment = server_streamer_pb2.StreamerServerSegment(segment = segment)
                )
                self.streamer_servicer_status[id]["segment_id"] += 1
            else:
                time.sleep(0.1)
                # self.logger.debug(f"Server sends heartbeat to client: {id}")
                yield server_streamer_pb2.StreamerServerMessage(
                    streamer_server_heartbeat=server_streamer_pb2.StreamerServerHeartbeat()
                )

            # OUTGOING PROCESSING START

            if len(self.outgoing[id]) > 0:
                mode, load = self.outgoing[id].popleft()
                
                if mode == "resend_info":
                    if not self.streamer_servicer_data[id][1] == None:
                        self.logger.info("Streamer send info to server.")
                        yield server_streamer_pb2.StreamerServerMessage(
                            streamer_server_info = server_streamer_pb2.StreamerServerInfo(info=self.streamer_servicer_data[id][1])
                        )
                        self.streamer_servicer_status[id]["status"] = "sent_info"
                    else:
                        self.outgoing[id].append(("resend_info", ""))
                
                elif mode == "new_source":
                    self.logger.info(f"STREAMER - {id} - main_thread - send streamer_server_new_source")
                    yield server_streamer_pb2.StreamerServerMessage(
                        streamer_server_new_source = server_streamer_pb2.StreamerServerNewSource(filename = load)
                    )


            
            # OUTGOING PROCESSING END