import grpc
import server_streamer_pb2, server_streamer_pb2_grpc
from video_segmenter import VideoSegmenter

import sys, time, threading, collections, random, concurrent, os, yaml, cv2, json
import numpy as np

class Streamer(server_streamer_pb2_grpc.ServerStreamerServiceServicer):

    def __init__(self, logger, source_id=0):
        self.logger = logger
        self.source_id = source_id
        self.sources = dict()
        self.pointer = -1
        self.current_data = b''
        self.info_str = ""
        self.info = ""
        # filename = input()

        self.server_status = dict()
        
        self.logger.info("Streamer starts video_segmenter.segment")
        # self.video_segmenter = VideoSegmenter(f"resource/{filename}", f"segment/{filename.split(".")[0]}/info.json", f"segment/{filename.split(".")[0]}", 1)
        # self.video_segmenter.segment()
        self.logger.info("Streamer ends video_segmenter.segment")

        self.logger.info("Streamer starts load_data")
        # self.load_data("segment/sao1")
        self.logger.info("Streamer ends load_data")

    def load_data(self, input_dir_path):
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
        if len(self.server_status) == 0:
            id = 0
            self.logger.info(f"First server joined to streamer.")
        else:
            id = random.randint(0, 10000)
            while id in self.server_status.keys():
                id = random.randint(0, 10000)
            self.logger.info(f"New server joined to streamer.")
        
        self.server_status[id] = [None, None, None, 0, 0]

        i = 0
        phase = 0

        def handle_requests():
            try:
                for message in request_iterator:
                    if(message.HasField("server_start_request")):
                        pass
                    elif(message.HasField("server_stop_request")):
                        pass
                    elif(message.HasField("server_source_request")):
                        VideoSegmenter(f"resource/{message.server_source_request.source}", f"segment/{message.server_source_request.source.split(".")[0]}/info.json", f"segment/{message.server_source_request.source.split(".")[0]}", 1).segment()
                        self.server_status[id][0], self.server_status[id][1], self.server_status[id][2]  = self.load_data("segment/" + message.server_source_request.source.split(".")[0])
            except grpc.RpcError as e:
                self.logger.error(f"RpcError")
        
        threading.Thread(target=handle_requests).start()



        while context.is_active():
            while self.server_status[id][0] == None or self.server_status[id][1] == None or self.server_status[id][2] == None:
                time.sleep(0.01)

            yield server_streamer_pb2.StreamerServerMessage(
                info=server_streamer_pb2.StreamerServerInfo(info=self.server_status[id][1])
            )

            while self.server_status[id][4] < len(self.server_status[id][0]):
                chunk = self.server_status[id][0][self.server_status[id][4]:(self.server_status[id][4]+self.server_status[id][2][self.server_status[id][3]])]
                self.server_status[id][4] += self.server_status[id][2][self.server_status[id][3]]
                self.server_status[id][3] += 1
                
                if len(chunk) == 0:
                    break
                self.logger.info(str(f"Send video chunk ({self.server_status[id][3]}) to server, timestamp: " + str(time.time())))
                yield server_streamer_pb2.StreamerServerMessage(
                    chunk=server_streamer_pb2.StreamerServerChunk(chunk=chunk)
                )
            
            self.server_status[id] = [None, None, None, 0, 0]