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
        self.name = {
            "sample": "sample.mp4"
        }
        self.source_availability = {
            "sample.mp4": True
        }
        
        self.info = None
        self.frame_id = 0
        self.source = None

        self.event_flag = {
            "pause_button_status": False,
            "unpause_button_status": False,
            "client_wants_source": False,
            "server_asks_for_status": False,
            "client_wants_to_upload": False
        }

        self.status_flag = {
            "start": True,
            "pause": False,
            "stop": False
        }
    
    def prepere_to_upload(self, filename):
        queue = collections.deque()
        with open(f"upload/{filename}", "rb") as f:
            buffer = f.read()
        phase = 0
        while phase + 2000000 < len(buffer):
            queue.append(buffer[phase:phase+2000000])
            phase += 2000000
        queue.append(buffer[phase:])
        del buffer
        return queue

    def generator(self):

        if self.status_flag["start"]:
            yield client_server_pb2.ClientServerMessage(
                client_server_start = client_server_pb2.ClientServerStart()
            )

        while not self.status_flag["stop"]:
            
            if self.event_flag["pause_button_status"]:
                self.logger.info(f"Client sends pause request to server. (self.frame_id = {self.frame_id})")
                yield client_server_pb2.ClientServerMessage(
                    client_server_pause = client_server_pb2.ClientServerPause(frame_id=str(self.frame_id))
                )
                self.event_flag["pause_button_status"] = False
            
            elif self.event_flag["client_wants_source"]:
                self.logger.info(f"Client sent choose_source_answer. (source = {str(self.source)})")
                if self.source in self.name.keys():
                    source = self.name[self.source]
                else:
                    source = self.source
                yield client_server_pb2.ClientServerMessage(
                    client_server_source = client_server_pb2.ClientServerSource(source=source)
                )
                self.event_flag["client_wants_source"] = False
            
            elif self.event_flag["unpause_button_status"]:
                self.logger.info(f"Client sends unpause request to server. (self.frame_id = {self.frame_id})")
                yield client_server_pb2.ClientServerMessage(
                    client_server_unpause = client_server_pb2.ClientServerUnpause(frame_id=str(self.frame_id))
                )
                self.event_flag["unpause_button_status"] = False
            
            elif self.event_flag["server_asks_for_status"]:
                self.logger.info(f"Client sends its status to server. (self.frame_id = {self.frame_id})")
                yield client_server_pb2.ClientServerMessage(
                    client_server_status_answer = client_server_pb2.ClientServerStatusAnswer(frame_id=str(self.frame_id))
                )
                self.event_flag["server_asks_for_status"] = False
            
            elif self.event_flag["client_wants_to_upload"]:
                self.logger.info("Client send client_upload_start to server.")
                yield client_server_pb2.ClientServerMessage(
                    client_server_upload_start = client_server_pb2.ClientServerUploadStart()
                )
                self.logger.info("Client starts prepere_to_upload.")
                queue = self.prepere_to_upload(self.upload_filename)
                self.name[self.upload_pseudoname] = self.upload_filename
                self.source_availability[self.upload_filename] = False
                self.logger.info("Client ends prepere_to_upload")
                while len(queue) > 0:
                    self.logger.debug("Client upload chunk to server")
                    yield client_server_pb2.ClientServerMessage(
                        client_server_upload_chunk = client_server_pb2.ClientServerUploadChunk(chunk = queue.popleft())
                    )
                self.logger.info("Client end upload to server")
                yield client_server_pb2.ClientServerMessage(
                    client_server_upload_end = client_server_pb2.ClientServerUploadEnd(filename = self.upload_filename)
                )
                self.event_flag["client_wants_to_upload"] = False
            
            time.sleep(0.1)
        
        yield client_server_pb2.ClientServerMessage(
            client_server_stop = client_server_pb2.ClientServerStop()
        )

    def server_connection(self):
        channel = connect_to_server(self.server_url, self.logger)
        stub = client_server_pb2_grpc.ClientServerServiceStub(channel)
        response_stream = stub.Stream(self.generator())
        try:
            for message in response_stream:
                if message.HasField("server_client_info"):
                    self.logger.info("Client got server_client_info")
                    self.info = json.loads(message.server_client_info.info)
                    self.queue.clear()
                    self.buffer = b''
                
                elif message.HasField("server_client_segment"):
                    self.logger.debug("Client got server_client_segment")
                    self.queue.append(message.server_client_segment.segment)
                
                elif message.HasField("server_client_pause"):
                    self.logger.info(f"Client got server_client_pause. (self.frame_id = {message.server_client_pause.frame_id})")
                    self.frame_id = int(message.server_client_pause.frame_id)
                    self.queue.clear()
                    self.buffer = b''
                    self.status_flag["pause"] = True
                
                elif message.HasField("server_client_unpause"):
                    self.logger.info(f"Client got server_client_unpause. (self.frame_id = {message.server_client_unpause.frame_id})")
                    self.frame_id =int(message.server_client_unpause.frame_id)
                    self.buffer = b''
                    self.queue.clear()
                    self.status_flag["pause"] = False
                
                elif message.HasField("server_client_status_request"):
                    self.logger.info("Client got server_client_status_request")
                    self.event_flag["server_asks_for_status"] = True
                
                elif message.HasField("server_client_new_source"):
                    self.logger.info(f"CLIENT - got server_client_new_source")
                    self.source_availability[message.server_client_new_source.filename] = True
                
                elif message.HasField("server_client_clear_info"):
                    self.logger.info(f"CLIENT - got server_client_clear_info")
                    self.info = None
                
                elif message.HasField("server_client_heartbeat"):
                    # self.logger.debug("Cleint got heartbeat from server.")
                    pass
                    
        except grpc.RpcError as e:
            self.logger.error(f"RpcError")
        finally:
            cv2.destroyAllWindows()

    def projection(self):
        pygame.init()

        screen = pygame.display.set_mode((1080, 1080), pygame.RESIZABLE)

        pygame.display.flip()
        
        clock = pygame.time.Clock()
        running = True

        while running:
            time.sleep(0.0001)
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    running = False
                if event.type == pygame.KEYDOWN:
                    if self.info != None:
                        if event.key == pygame.K_SPACE:
                            if self.status_flag["pause"]:
                                self.event_flag["unpause_button_status"] = True
                            else:
                                self.event_flag["pause_button_status"] = True
                    if event.key == pygame.K_q or event.key == pygame.QUIT:
                        self.status_flag["stop"] = True
                        running = False
                    if event.key == pygame.K_s:
                        print(f"Please type in name of file you want to watch from list below:")
                        for key in self.name.keys():
                            print(f"{key}")
                        source = input()
                        if source not in self.name.keys():
                            print(f"No {source} in list above.")
                        else:
                            if self.source_availability[self.name[source]]:
                                self.source = source
                                self.event_flag["client_wants_source"] = True
                            else:
                                self.logger.warning(f"CLIENT - source is not available")
                    if event.key == pygame.K_l:
                        print(f"Please type in filename of file you want to uplaod:")
                        upload_filename = input()
                        print(f"Please type in pseudoname of this file:")
                        upload_pseudoname = input()

                        if upload_filename not in os.listdir("upload/"):
                            print(f"There is no {upload_filename} in upload/ directory")
                        else:
                            self.upload_filename = upload_filename
                            self.upload_pseudoname = upload_pseudoname
                            self.event_flag["client_wants_to_upload"] = True

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

                    window_width, window_height = screen.get_size()
                    source_width, source_height = self.info[-1][0], self.info[-1][1]
                    scale = min(window_width / source_width, window_height / source_height)
                    new_width, new_height = int(source_width * scale), int(source_height * scale)

                    frame = cv2.resize(frame, (int(source_width * scale), int(source_height * scale)), interpolation=cv2.INTER_AREA)

                    result = np.zeros((window_height, window_width, 3), dtype=np.uint8)

                    x_offset = (window_width - new_width) // 2
                    y_offset = (window_height - new_height) // 2

                    result[y_offset:y_offset+new_height, x_offset:x_offset+new_width] = frame

                    frame = cv2.cvtColor(result, cv2.COLOR_BGR2RGB)
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
                            if self.info != None:
                                if event.key == pygame.K_SPACE:
                                    if self.status_flag["pause"]:
                                        self.event_flag["unpause_button_status"] = True
                                    else:
                                        self.event_flag["pause_button_status"] = True
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