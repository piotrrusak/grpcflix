import grpc
import client_server_pb2, client_server_pb2_grpc

import sys, time, threading, collections, cv2, multiprocessing, yaml, os, pygame, json, tempfile
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
        self.info = None

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
            yield client_server_pb2.ClientServerMessage(
                client_start_request=client_server_pb2.ClientStartRequest(timestamp=str(self.timestamp))
            )
            self.start = False

        while not self.stop:
            while self.pause_on_id == 0 and self.unpause_on_id == 0:
                time.sleep(0.01)
            if self.pause_on_id:
                logger.info("Client sends pause request to server.")
                yield client_server_pb2.ClientServerMessage(
                    client_pause_request=client_server_pb2.ClientPauseRequest(timestamp=str(self.frame_id))
                )
                self.pause_on_id = 0
            if self.unpause_on_id:
                logger.info("Client sends unpause request to server")
                yield client_server_pb2.ClientServerMessage(
                    client_unpause_request=client_server_pb2.ClientUnpauseRequest(timestamp=str(self.frame_id))
                )
                self.unpause_on_id = 0
        logger.info("Client stops.")
        yield client_server_pb2.ClientServerMessage(
            client_stop_request=client_server_pb2.ClientStopRequest(reason="user_cancelled")
        )
    
    def buffer_to_queue(self):
        while self.info is None:
            time.sleep(0.01)
        for segment in self.info:
            print(segment)
            while len(self.buffer) < segment[0]:
                time.sleep(0.01)
            self.queue.append(self.buffer[:segment[0]])
            self.buffer = self.buffer[segment[0]:]

    def server_connection(self):
        logger.info("server_connection")
        channel = connect_to_server(self.server_url)
        stub = client_server_pb2_grpc.ClientServerServiceStub(channel)
        response_stream = stub.Stream(self.generator())
        try:
            for message in response_stream:
                if message.HasField("info"):
                    self.info = json.loads(message.info.info)
                if message.HasField("chunk"):
                    self.buffer += message.chunk.chunk
                if message.HasField("server_pause_request"):
                    self.pause = 1
                if message.HasField("server_unpause_request"):
                    self.pause = 0
                    self.queue.clear()
        except grpc.RpcError as e:
            print(f"RpcError: {e.code()} - {e.details()}.")
        finally:
            cv2.destroyAllWindows()

    def projection(self):
        pygame.init()
        screen = pygame.display.set_mode((1080, 1080))
        pygame.display.flip()
        
        clock = pygame.time.Clock()
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
                video_bytes = self.queue.popleft()
                logger.info("Next segment")

                # Zapis chunku do tymczasowego pliku
                with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp_file:
                    tmp_file.write(video_bytes)
                    tmp_path = tmp_file.name

                cap = cv2.VideoCapture(tmp_path)
                if not cap.isOpened():
                    logger.warning("Nie można otworzyć chunku video.")
                    os.remove(tmp_path)
                    continue

                while cap.isOpened():
                    ret, frame = cap.read()
                    if not ret:
                        break

                    frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    frame = np.transpose(frame, (1, 0, 2))
                    surface = pygame.surfarray.make_surface(frame)
                    screen.blit(surface, (0, 0))
                    pygame.display.flip()
                    clock.tick(60)

                    for event in pygame.event.get():
                        if event.type == pygame.QUIT:
                            running = False
                            break
                        if event.type == pygame.KEYDOWN:
                            if event.key == pygame.K_p:
                                logger.info("PAUSE")
                                self.pause_on_id = 1
                                self.pause = 1
                            if event.key == pygame.K_u:
                                logger.info("UNPAUSE")
                                self.unpause_on_id = 1
                                self.pause = 0

                    if self.pause:
                        break

                cap.release()
                os.remove(tmp_path)

        pygame.quit()


if __name__ == '__main__':
    client = Client()

    threading.Thread(target=client.server_connection).start()
    threading.Thread(target=client.buffer_to_queue).start()

    client.projection()