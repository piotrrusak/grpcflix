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
        while self.streamer_servicer_status[id] != "initialised":
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
        
        self.streamer_servicer_status[id] = "initialised"
        self.streamer_servicer_data[id] = [collections.deque(), None, None]
        self.outgoing[id] = collections.deque()
        self.active_loaders[id] = 0

        def handle_requests(self):
            try:
                for message in request_iterator:

                    if(message.HasField("server_start_request")):
                        pass

                    elif(message.HasField("server_stop_request")):
                        pass
                    
                    elif(message.HasField("server_source_request")):
                        if not os.path.exists(f"resource/{message.server_source_request.source}"):
                            self.logger.warning(f"No such source as: {message.server_source_request.source}")
                            self.outgoing[id].append(("no_such_file", ""))
                            continue
                        
                        self.active_loaders[id] += 1
                        while self.active_loaders[id] != 1:
                            time.sleep(0.01)
                        
                        self.streamer_servicer_data[id][0] = collections.deque()
                        self.streamer_servicer_data[id][1] = None
                        self.streamer_servicer_data[id][2] = None
                        self.streamer_servicer_status[id] = "initialised"
                        threading.Thread(target=self.load_data, args=(id, "segment/" + message.server_source_request.source.split(".")[0])).start()

                    # UPLOAD START

                    elif(message.HasField("server_upload_start")):
                        upload_buffer = b''
                    
                    elif(message.HasField("server_upload_chunk")):
                        upload_buffer += message.server_upload_chunk.chunk
                    
                    elif(message.HasField("server_upload_end")):
                        with open(f"resource/{message.server_upload_end.filename}", 'wb') as f:
                            self.logger.info(f"Write in file: {message.server_upload_end.filename}")
                            f.write(upload_buffer)
                            del upload_buffer
                        if not os.path.exists(f"segment/{message.server_upload_end.filename.split(".")[0]}/info.json"):
                            self.logger.info(f"Not found segments of: {message.server_upload_end.filename.split(".")[0]}. Segmentation starts.")
                            threading.Thread(target=VideoSegmenter(f"resource/{message.server_upload_end.filename}", f"segment/{message.server_upload_end.filename.split(".")[0]}/info.json", f"segment/{message.server_upload_end.filename.split(".")[0]}", 1).segment).start()

                    
                    # UPLOAD END 
            
            except grpc.RpcError as e:
                self.logger.error(f"RpcError")
        
        threading.Thread(target=handle_requests, args=(self,)).start()

        while context.is_active():
            
            if self.streamer_servicer_status[id] == "initialised" and not self.streamer_servicer_data[id][1] == None and not self.streamer_servicer_data[id][2] == None:
                self.logger.info("Streamer send info to server.")
                yield server_streamer_pb2.StreamerServerMessage(
                    info=server_streamer_pb2.StreamerServerInfo(info=self.streamer_servicer_data[id][1])
                )
                self.streamer_servicer_status[id] = "sent_info"

            elif self.streamer_servicer_status[id] == "sent_info":
                self.logger.info("Streamer start data transfer to server.")
                i = 0
                while self.streamer_servicer_status[id] == "sent_info" and i < len(self.streamer_servicer_data[id][2]):
                    if len(self.streamer_servicer_data[id][0]) > 0:
                        chunk = self.streamer_servicer_data[id][0].popleft()
                        if len(chunk) == 0:
                            break
                        self.logger.debug(str(f"Send segment no.: {i} to server."))
                        yield server_streamer_pb2.StreamerServerMessage(
                            chunk=server_streamer_pb2.StreamerServerChunk(chunk=chunk)
                        )
                        i += 1
                if self.streamer_servicer_status[id] == "sent_info":
                    self.streamer_servicer_status[id] = "sent_data"
                    self.logger.info("Streamer finished data transfer to server.") 

            # OUTGOING PROCESSING START

            if len(self.outgoing[id]) > 0:
                while len(self.outgoing[id]) > 0:
                    typ, load = self.outgoing[id].popleft()

                    if typ == "no_such_file":
                        self.logger.info(f"Streamer sends no_such_file to")
                        yield server_streamer_pb2.StreamerServerMessage(
                            streamer_no_such_file_request = server_streamer_pb2.StreamerNoSuchFileRequest()
                        )
                continue
            
            # OUTGOING PROCESSING END