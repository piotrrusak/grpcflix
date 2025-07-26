import grpc
import client_server_pb2, client_server_pb2_grpc

import sys, time, threading, collections, cv2, multiprocessing, yaml, os, pygame
import numpy as np

import logging

logger = logging.getLogger(__name__)

# logger.setLevel(logging.NOTSET / logging.DEBUG / logging.INFO / logging.WARNING / logging.ERROR / logging.CRITICAL)
# Sets the minimum level of logs that will be taken into account (i.e., processed).
# If not set, have value of logging.NOTSET, then logger takes value of root logger i.e. logging.WARNING.
logger.setLevel(logging.INFO)

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

def connect_to_server(address, retries=10, delay=2):
    for attempt in range(retries):
        try:
            logger.info(f"Connecting to server at {address} (attempt {attempt + 1})")
            channel = grpc.insecure_channel(address)
            grpc.channel_ready_future(channel).result(timeout=3)
            logger.info("Successfully connected to server!")
            return channel
        except grpc.FutureTimeoutError:
            logger.warning(f"Server not ready yet, retrying in {delay} seconds...")
            time.sleep(delay)
    raise ConnectionError(f"Failed to connect to server at {address} after {retries} attempts.")

class Client:

    def __init__(
                 self,
                 server_url_docker='server:50001',
                 server_url='localhost:50001',
                 ):
        self.start = 1
        self.timestamp = 0

        self.frame_id = 0
        
        self.stop = 0
        self.pause = 0
        self.pause_on_id = 0
        self.unpause_on_id = 0

        self.queue = collections.deque()
        self.buffer = b''

        if config["docker"] == "True":
            self.server_url = server_url_docker
        else:
            self.server_url = server_url

    def generator(self):
        if self.start:
            yield client_server_pb2.ClientMessage(
                start=client_server_pb2.StartRequest(timestamp=str(self.timestamp))
            )
            self.start = False

        while not self.stop:
            while self.pause_on_id == 0 and self.unpause_on_id == 0:
                time.sleep(0.01)
            if self.pause_on_id:
                logger.info("Client sends pause request to server.")
                yield client_server_pb2.ClientMessage(
                    pause=client_server_pb2.PauseRequest(timestamp=str(self.timestamp))
                )
                self.pause_on_id = 0
            if self.unpause_on_id:
                logger.info("Client sends unpause request to server")
                yield client_server_pb2.ClientMessage(
                    unpause=client_server_pb2.UnpauseRequest(timestamp=str(self.timestamp))
                )
                self.unpause_on_id = 0
        logger.info("Client stops.")
        yield client_server_pb2.ClientMessage(
            stop=client_server_pb2.StopRequest(reason="user_cancelled")
        )
        
    def server_connection(self):
        logger.info("server_connection")
        channel = connect_to_server(self.server_url)
        stub = client_server_pb2_grpc.ClientServerServiceStub(channel)
        response_stream = stub.Stream(self.generator())
        buffer = b''
        try:
            for chunk in response_stream:
                buffer += chunk.video_chunk.frames
                if chunk.HasField("video_chunk"):
                    while len(buffer) > (1080 * 1080 * 3):
                        self.queue.append(buffer[:1080*1080*3])
                        buffer = buffer[1080*1080*3:]
                if chunk.HasField("server_pause_request"):
                    self.pause = 1
                if chunk.HasField("server_unpause_request"):
                    self.pause = 0
        except grpc.RpcError as e:
            print(f"RpcError: {e.code()} - {e.details()}.")
        finally:
            cv2.destroyAllWindows()
        
    def projection(self):
        pygame.init()
        screen = pygame.display.set_mode((1080, 1080))
        pygame.display.flip()
        
        clock = pygame.time.Clock()  # To control frame rate
        running = True
        
        while running:
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    running = False
                if event.type == pygame.KEYDOWN:
                    if event.key == pygame.K_p:
                        logger.info("PAUSE")
                        self.pause_on_id = 1
                        self.pause = 1
                    if event.key == pygame.K_u:
                        logger.info("UNPAUSE")
                        self.unpause_on_id = 1
                        self.pause = 0

            if not self.pause and len(self.queue) > 0:
                frame_bytes = self.queue.popleft()
                logger.info("Show frame.")
                
                # Convert frame bytes to numpy array and reshape to the correct dimensions
                frame = np.frombuffer(frame_bytes, dtype=np.uint8).reshape((1080, 1080, 3))
                frame_surface = pygame.surfarray.make_surface(np.transpose(frame, (1, 0, 2)))
                screen.blit(frame_surface, (0, 0))
                pygame.display.flip()  # Update the screen
                self.timestamp += 1
                clock.tick(60)  # Limit frame rate to 60 FPS
        
        pygame.quit()
        sys.exit()
            #     logger.info("could")
            #     cv2.imshow("grpcflix", frame)
            #     key = cv2.waitKey(1)
            #     if key != -1:
            #         if key == ord('q'):
            #             self.stop = True
            #             logger.info("Detected q")
            #         elif key == ord('p'):
            #             logger.info("Detected p")
            #             self.pause_on_id = 1
            #         elif key == ord('i'):
            #             logger.info("Detected i")
            #             self.unpause_on_id = 1
            #     logger.info("Current key value: " + str(key))
            # else:
            #     key = cv2.waitKey(int(1000 / 60))
            #     if key != -1:
            #         if key == ord('q'):
            #             self.stop = True
            #             logger.info("Detected q")
            #         elif key == ord('p'):
            #             logger.info("Detected p")
            #             self.pause_on_id = 1
            #         elif key == ord('i'):
            #             logger.info("Detected i")
            #             self.unpause_on_id = 1
        


if __name__ == '__main__':
    client = Client()

    threading.Thread(target=client.server_connection).start()
    
    client.projection()