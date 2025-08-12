import grpc, server_streamer_pb2_grpc

from streamer import Streamer
from video_segmenter import VideoSegmenter

import logging, sys, concurrent

logger = logging.getLogger(__name__)

# logger.setLevel(logging.NOTSET / logging.DEBUG / logging.INFO / logging.WARNING / logging.ERROR / logging.CRITICAL)
# Sets the minimum level of logs that will be taken into account (i.e., processed).
# If not set, have value of logging.NOTSET, then logger takes value of root logger i.e. logging.WARNING.
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler("streamer.log", mode='w')
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Because of root logger (every custom logger is child of root logger)
logger.propagate = False

def serve():
    streamer = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    server_streamer_pb2_grpc.add_ServerStreamerServiceServicer_to_server(Streamer(logger), streamer)
    streamer.add_insecure_port('0.0.0.0:50002')
    streamer.start()
    streamer.wait_for_termination()

if __name__ == '__main__':
    serve()