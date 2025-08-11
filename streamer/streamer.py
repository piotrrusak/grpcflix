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

    def load_data(self, input_dir_path):
        self.logger.info(f"Streamer starts load_data: {input_dir_path}")
        current_data = b''
        for filename in sorted(os.listdir(input_dir_path)):
            if filename == "info.json":
                continue
            with open(os.path.join(input_dir_path, filename), 'rb') as f:
                current_data += f.read()
        with open(os.path.join(input_dir_path, "info.json"), 'r') as f:
            info_str = f.read()
        with open(os.path.join(input_dir_path, "info.json"), 'r') as f:
            info = json.load(f)
        return current_data, info_str, info

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
        self.streamer_servicer_data[id] = [None, None, None, 0, 0]
        self.outgoing[id] = collections.deque()

        def handle_requests():
            try:
                for message in request_iterator:
                    if(message.HasField("server_start_request")):
                        pass
                    elif(message.HasField("server_stop_request")):
                        pass
                    elif(message.HasField("server_source_request")):
                        self.streamer_servicer_status[id] = "initialised"
                        if not os.path.exists(f"resource/{message.server_source_request.source}"):
                            self.logger.warning(f"No such source as: {message.server_source_request.source}")
                            self.outgoing[id].append(("no_such_file", ""))
                            continue
                        if not os.path.exists(f"segment/{message.server_source_request.source.split(".")[0]}/info.json"):
                            VideoSegmenter(f"resource/{message.server_source_request.source}", f"segment/{message.server_source_request.source.split(".")[0]}/info.json", f"segment/{message.server_source_request.source.split(".")[0]}", 1).segment()
                        self.streamer_servicer_data[id][0], self.streamer_servicer_data[id][1], self.streamer_servicer_data[id][2]  = self.load_data("segment/" + message.server_source_request.source.split(".")[0])
                    elif(message.HasField("server_upload_start")):
                        upload_buffer = b''
                    elif(message.HasField("server_upload_chunk")):
                        upload_buffer += message.server_upload_chunk.chunk
                    elif(message.HasField("server_upload_end")):
                        with open("resource/1.mp4", 'wb') as f:
                            f.write(upload_buffer)
                            del upload_buffer
            
            except grpc.RpcError as e:
                self.logger.error(f"RpcError")
        
        threading.Thread(target=handle_requests).start()



        while context.is_active():

            if self.streamer_servicer_status[id] == "initialised" and not self.streamer_servicer_data[id][1] == None and not self.streamer_servicer_data[id][2] == None:
                yield server_streamer_pb2.StreamerServerMessage(
                    info=server_streamer_pb2.StreamerServerInfo(info=self.streamer_servicer_data[id][1])
                )
                self.streamer_servicer_status[id] = "sent_info"

            if self.streamer_servicer_status[id] == "sent_info" and not self.streamer_servicer_data[id][0] == None:
                while self.streamer_servicer_data[id][4] < len(self.streamer_servicer_data[id][0]):
                    chunk = self.streamer_servicer_data[id][0][self.streamer_servicer_data[id][4]:(self.streamer_servicer_data[id][4]+self.streamer_servicer_data[id][2][self.streamer_servicer_data[id][3]])]
                    self.streamer_servicer_data[id][4] += self.streamer_servicer_data[id][2][self.streamer_servicer_data[id][3]]
                    self.streamer_servicer_data[id][3] += 1
                    
                    if len(chunk) == 0:
                        break
                    self.logger.debug(str(f"Send segment no.: {self.streamer_servicer_data[id][3]} to server."))
                    yield server_streamer_pb2.StreamerServerMessage(
                        chunk=server_streamer_pb2.StreamerServerChunk(chunk=chunk)
                    )
                
                self.streamer_servicer_data[id] = [None, None, None, 0, 0]
                self.streamer_servicer_status[id] = "sent_data"
                self.logger.info("Streamer finished data transfer to server.")

            if len(self.outgoing[id]) != 0:
                self.logger.info(f"Streamer sends no_such_file to")
                while len(self.outgoing[id]) > 0:
                    typ, load = self.outgoing[id].popleft()

                    if typ == "no_such_file":
                        yield server_streamer_pb2.StreamerServerMessage(
                            streamer_no_such_file_request = server_streamer_pb2.StreamerNoSuchFileRequest()
                        )
                continue
