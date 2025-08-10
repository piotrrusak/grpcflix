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

    def __init__(self, logger, server_url='localhost:50001'):
        self.logger = logger
        self.queue = collections.deque()
        self.buffer = b''
        self.server_url = server_url
        
        self.info = None
        self.frame_id = 0

        self.event_flag = {
            "pause_button_status": False,
            "unpause_button_status": False,
            "server_asks_for_source": False,
            "server_asks_for_status": False
        }

        self.status_flag = {
            "start": True,
            "pause": False,
            "stop": False
        }

    def generator(self):

        if self.status_flag["start"]:
            yield client_server_pb2.ClientServerMessage(
                client_start_request=client_server_pb2.ClientStartRequest()
            )

        while not self.status_flag["stop"]:
            while not self.event_flag["pause_button_status"] and not self.event_flag["unpause_button_status"] and not self.event_flag["server_asks_for_status"] and not self.event_flag["server_asks_for_source"]:
                if self.status_flag["stop"]:
                    self.logger.info("Client sends client_stop_request.")
                    yield client_server_pb2.ClientServerMessage(
                        client_stop_request=client_server_pb2.ClientStopRequest()
                    )
                time.sleep(0.1)
            if self.event_flag["pause_button_status"]:
                self.logger.info(f"Client sends pause request to server. (self.frame_id = {self.frame_id})")
                yield client_server_pb2.ClientServerMessage(
                    client_pause_request=client_server_pb2.ClientPauseRequest(frame_id=str(self.frame_id))
                )
                self.event_flag["pause_button_status"] = False
            elif self.event_flag["server_asks_for_source"]:
                self.logger.info("Enter name of file:")
                source = str(input())
                self.logger.info(f"Client sent choose_source_answer. (source = {source})")
                yield client_server_pb2.ClientServerMessage(
                    client_choose_source_answer = client_server_pb2.ClientChooseSourceAnswer(source=source)
                )
                self.event_flag["server_asks_for_source"] = False
            elif self.event_flag["unpause_button_status"]:
                self.logger.info(f"Client sends unpause request to server. (self.frame_id = {self.frame_id})")
                yield client_server_pb2.ClientServerMessage(
                    client_unpause_request=client_server_pb2.ClientUnpauseRequest(frame_id=str(self.frame_id))
                )
                self.event_flag["unpause_button_status"] = False
            elif self.event_flag["server_asks_for_status"]:
                self.logger.info(f"Client sends its status to server. (self.frame_id = {self.frame_id})")
                yield client_server_pb2.ClientServerMessage(
                    client_status_answer=client_server_pb2.ClientStatusAnswer(frame_id=str(self.frame_id))
                )
                self.event_flag["server_asks_for_status"] = False
            else:
                self.logger.info("Client sends heartbeat to server")
                yield client_server_pb2.ClientServerMessage(
                    heartbeat=client_server_pb2.Heartbeat()
                )
    
    def buffer_to_queue(self, start):
        self.logger.info(f"Client buffer_to_queue starts from segment no.: {start}")
        while self.info is None:
            time.sleep(0.01)
        for i in range(len(self.info)-1):
            if i >= start:
                self.logger.debug(f"Client process chunk: {i}. (len(self.buffer), len(self.queue)) = ({len(self.buffer)}, {len(self.queue)})")
                while len(self.buffer) < self.info[i]:
                    time.sleep(0.01)
                self.queue.append(self.buffer[:self.info[i]])
                self.buffer = self.buffer[self.info[i]:]

    def server_connection(self):
        channel = connect_to_server(self.server_url, self.logger)
        stub = client_server_pb2_grpc.ClientServerServiceStub(channel)
        response_stream = stub.Stream(self.generator())
        try:
            for message in response_stream:
                if message.HasField("info"):
                    self.logger.info("Client got info")
                    self.info = json.loads(message.info.info)
                if message.HasField("server_choose_source_request"):
                    self.event_flag["server_asks_for_source"] = True
                    self.logger.info("Client got server_choose_source_request")
                if message.HasField("chunk"):
                    self.logger.debug("Client got chunk")
                    self.queue.append(message.chunk.chunk)
                if message.HasField("server_pause_request"):
                    self.logger.info(f"Client got server_pause_request. (self.frame_id = {message.server_pause_request.frame_id})")
                    self.frame_id = int(message.server_pause_request.frame_id)
                    self.queue.clear()
                    self.buffer = b''
                    self.status_flag["pause"] = True
                if message.HasField("server_unpause_request"):
                    self.logger.info(f"Client got server_unpause_request. (self.frame_id = {message.server_unpause_request.frame_id})")
                    self.frame_id =int(message.server_unpause_request.frame_id)
                    self.buffer = b''
                    self.queue.clear()
                    self.status_flag["pause"] = False
                    threading.Thread(target=self.buffer_to_queue, args=(int(message.server_unpause_request.frame_id)//int(round(self.info[-1][2])),)).start()
                if message.HasField("server_status_request"):
                    self.logger.info("Client got server_status_request")
                    self.event_flag["server_asks_for_status"] = True
                if message.HasField("heartbeat"):
                    # self.logger.info("heartbeat")
                    pass
                    
        except grpc.RpcError as e:
            self.logger.error(f"RpcError")
        finally:
            cv2.destroyAllWindows()

    def projection(self):
        pygame.init()

        while self.info == None:
            time.sleep(0.01)
        screen = pygame.display.set_mode((self.info[-1][0], self.info[-1][1]), pygame.RESIZABLE)

        pygame.display.flip()
        
        clock = pygame.time.Clock()
        running = True

        while running:
            time.sleep(0.0001)
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    running = False
                if event.type == pygame.KEYDOWN:
                    if event.key == pygame.K_p:
                        self.event_flag["pause_button_status"] = True
                        self.status_flag["pause"] = True
                    if event.key == pygame.K_u:
                        self.event_flag["unpause_button_status"] = True
                        self.status_flag["pause"] = False
                    if event.key == pygame.K_q or event.key == pygame.QUIT:
                        self.status_flag["stop"] = True
                        running = False

            if not self.status_flag["pause"] and len(self.queue) > 0:

                video_bytes = self.queue.popleft()

                with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp_file:
                    tmp_file.write(video_bytes)
                    tmp_path = tmp_file.name

                cap = cv2.VideoCapture(tmp_path)
                cap.set(cv2.CAP_PROP_POS_FRAMES, self.frame_id%int(round(self.info[-1][2])))
                if not cap.isOpened():
                    self.logger.warning("Nie można otworzyć chunku video.")
                    os.remove(tmp_path)
                    continue
                
                while cap.isOpened():
                    self.frame_id += 1
                    ret, frame = cap.read()
                    if not ret:
                        break
                    width, height = screen.get_size()
                    frame = cv2.resize(frame, (width, height), interpolation=cv2.INTER_AREA)

                    frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    frame = np.transpose(frame, (1, 0, 2))
                    surface = pygame.surfarray.make_surface(frame)
                    screen.blit(surface, (0, 0))
                    pygame.display.flip()
                    clock.tick(int(round(self.info[-1][2])))

                    for event in pygame.event.get():
                        if event.type == pygame.QUIT:
                            running = False
                            break
                        if event.type == pygame.KEYDOWN:
                            if event.key == pygame.K_p:
                                self.event_flag["pause_button_status"] = True
                                self.status_flag["pause"] = True
                            if event.key == pygame.K_u:
                                self.event_flag["unpause_button_status"] = True
                                self.status_flag["pause"] = False
                            if event.key == pygame.K_q or event.key == pygame.QUIT:
                                self.event_flag["pause_button_status"] = True
                                self.status_flag["stop"] = True
                                running = False

                    if self.status_flag["pause"]:
                        break

                cap.release()
                os.remove(tmp_path)
        
        time.sleep(1)

        pygame.quit()