import grpc, client_server_pb2_grpc

from server import Servicer

import logging, sys, concurrent

logger = logging.getLogger(__name__)

# logger.setLevel(logging.NOTSET / logging.DEBUG / logging.INFO / logging.WARNING / logging.ERROR / logging.CRITICAL)
# Sets the minimum level of logs that will be taken into account (i.e., processed).
# If not set, have value of logging.NOTSET, then logger takes value of root logger i.e. logging.WARNING.
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler("server.log", mode='w')
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Because of root logger (every custom logger is child of root logger)
logger.propagate = False
 
def serve():
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    client_server_pb2_grpc.add_ClientServerServiceServicer_to_server(Servicer(logger), server)
    server.add_insecure_port('0.0.0.0:50001')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()