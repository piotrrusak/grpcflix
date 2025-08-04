import grpc
import client_server_pb2, client_server_pb2_grpc

import sys, time, threading, collections, cv2, multiprocessing, yaml, os, pygame, json, tempfile
import numpy as np


def connect_to_server(address, logger, retries=10, delay=2):
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

    def __init__(self, logger, server_url_docker='server:50001', server_url='localhost:50001'):
        self.logger = logger
        self.queue = collections.deque()
        self.buffer = b''
        self.server_url = server_url

        self.start = 1
        
        self.info = None
        self.frame_id = 0
        self.pause = 0

        self.pause_button_status = 0
        self.unpause_button_status = 0
        
        self.status_request = 0

        self.stop = 0

    def generator(self):

        if self.start:
            yield client_server_pb2.ClientServerMessage(
                client_start_request=client_server_pb2.ClientStartRequest()
            )

        while not self.stop:
            while self.pause_button_status == 0 and self.unpause_button_status == 0 and self.status_request == 0:
                time.sleep(0.1)
            if self.pause_button_status:
                self.logger.info("Client sends pause request to server.")
                yield client_server_pb2.ClientServerMessage(
                    client_pause_request=client_server_pb2.ClientPauseRequest(timestamp=str(self.frame_id))
                )
                self.pause_button_status = 0
            elif self.unpause_button_status:
                self.logger.info("Client sends unpause request to server.")
                yield client_server_pb2.ClientServerMessage(
                    client_unpause_request=client_server_pb2.ClientUnpauseRequest(timestamp=str(self.frame_id))
                )
                self.unpause_button_status = 0
            elif self.status_request:
                self.logger.info("Client sends its status to server")
                yield client_server_pb2.ClientServerMessage(
                    client_status_answer=client_server_pb2.ClientStatusAnswer(frame_id=str(self.frame_id))
                )
                self.status_request = 0
            else:
                self.logger.info("Client sends heartbeat to server")
                yield client_server_pb2.ClientServerMessage(
                    heartbeat=client_server_pb2.Heartbeat()
                )
        self.logger.info("Client stops.")
        yield client_server_pb2.ClientServerMessage(
            client_stop_request=client_server_pb2.ClientStopRequest()
        )
    
    def buffer_to_queue(self, start):
        self.logger.info(f"CLIENT RESTARTS FORM {start}")
        while self.info is None:
            time.sleep(0.01)
        for i in range(len(self.info)):
            if i >= start:
                while len(self.buffer) < self.info[i][0]:
                    time.sleep(0.01)
                self.queue.append(self.buffer[:self.info[i][0]])
                self.buffer = self.buffer[self.info[i][0]:]

    def server_connection(self):
        channel = connect_to_server(self.server_url, self.logger)
        stub = client_server_pb2_grpc.ClientServerServiceStub(channel)
        response_stream = stub.Stream(self.generator())
        try:
            for message in response_stream:
                if message.HasField("info"):
                    self.logger.info("Client got info")
                    self.info = json.loads(message.info.info)
                if message.HasField("chunk"):
                    self.logger.info("Client got chunk")
                    self.buffer += message.chunk.chunk
                if message.HasField("server_pause_request"):
                    self.logger.info("Client got server_pause_request")
                    self.pause = 1
                    self.queue.clear()
                    self.buffer = b''
                    self.frame_id = int(message.server_pause_request.timestamp)
                if message.HasField("server_unpause_request"):
                    self.logger.info("Client got server_unpause_request")
                    threading.Thread(target=self.buffer_to_queue, args=(int(message.server_unpause_request.timestamp),)).start()
                    self.queue.clear()
                    self.pause = 0
                if message.HasField("server_status_request"):
                    self.logger.info("Client got server_status_request")
                    self.status_request = 1
                if message.HasField("heartbeat"):
                    # self.logger.info("heartbeat")
                    pass
                    
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
            time.sleep(0.1)
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    running = False
                if event.type == pygame.KEYDOWN:
                    if event.key == pygame.K_p:
                        self.logger.info("PAUSE")
                        self.pause_button_status = 1
                        self.pause = 1
                    if event.key == pygame.K_u:
                        self.logger.info("UNPAUSE")
                        self.unpause_button_status = 1
                        self.pause = 0

            if not self.pause and len(self.queue) > 0:
                video_bytes = self.queue.popleft()

                with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp_file:
                    tmp_file.write(video_bytes)
                    tmp_path = tmp_file.name

                cap = cv2.VideoCapture(tmp_path)
                if not cap.isOpened():
                    self.logger.warning("Nie można otworzyć chunku video.")
                    os.remove(tmp_path)
                    continue
                
                while cap.isOpened():
                    self.frame_id += 1
                    ret, frame = cap.read()
                    if not ret:
                        break

                    frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    frame = np.transpose(frame, (1, 0, 2))
                    surface = pygame.surfarray.make_surface(frame)
                    screen.blit(surface, (0, 0))
                    pygame.display.flip()
                    clock.tick(60)
                    time.sleep(0.01) 

                    for event in pygame.event.get():
                        if event.type == pygame.QUIT:
                            running = False
                            break
                        if event.type == pygame.KEYDOWN:
                            if event.key == pygame.K_p:
                                self.logger.info("PAUSE")
                                self.pause_button_status = 1
                                self.pause = 1
                            if event.key == pygame.K_u:
                                self.logger.info("UNPAUSE")
                                self.unpause_button_status = 1
                                self.pause = 0

                    if self.pause:
                        break

                cap.release()
                os.remove(tmp_path)

        pygame.quit()
