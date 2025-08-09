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
        filename = "sao1.mp4"
        
        self.logger.info("Streamer starts video_segmenter.segment")
        self.video_segmenter = VideoSegmenter(f"resource/{filename}", f"segment/{filename.split(".")[0]}/info.json", f"segment/{filename.split(".")[0]}", 1)
        self.video_segmenter.segment()
        self.logger.info("Streamer ends video_segmenter.segment")

        self.logger.info("Streamer starts load_data")
        self.load_data("segment/sao1")
        self.logger.info("Streamer ends load_data")

    def load_data(self, input_dir_path):
        self.current_data = b''
        self.info = ""
        for filename in sorted(os.listdir(input_dir_path)):
            if filename == "info.json":
                continue
            with open(os.path.join(input_dir_path, filename), 'rb') as f:
                self.current_data += f.read()
        with open(os.path.join(input_dir_path, "info.json"), 'r') as f:
            self.info_str += f.read()
        with open(os.path.join(input_dir_path, "info.json"), 'r') as f:
            self.info = json.load(f)
        

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
            info=server_streamer_pb2.StreamerServerInfo(info=self.info_str)
        )

        i = 0
        phase = 0
        while context.is_active() and phase < len(self.current_data):
            chunk = self.current_data[phase:(phase+self.info[i])]
            phase += self.info[i]
            i += 1
            
            if len(chunk) == 0:
                break
            self.logger.info(str(f"Send video chunk ({self.pointer}) to server, timestamp: " + str(time.time())))
            yield server_streamer_pb2.StreamerServerMessage(
                chunk=server_streamer_pb2.StreamerServerChunk(chunk=chunk)
            )

            if len(requests) != 0:
                print(requests)
                requests.clear()