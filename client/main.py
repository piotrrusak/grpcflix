import grpc
import video_pb2
import video_pb2_grpc
import time
import threading
from collections import deque

import cv2
import numpy as np

class Client:

    def __init__(self):
        self.start = 1
        self.timestamp = 0
        self.stop = 0
        self.queue = deque()

    def generator(self):
        if self.start:
            yield video_pb2.ClientMessage(
                start=video_pb2.StartRequest(timestamp=str(self.timestamp))
            )
            self.start = False

        while not self.stop:
            time.sleep(0.5)
        yield video_pb2.ClientMessage(
            stop=video_pb2.StopRequest(reason="user_cancelled")
        )
    
    def server_connection(self):
        print("server_connection")
        with grpc.insecure_channel('server:50001') as channel:
            stub = video_pb2_grpc.VideoServiceStub(channel)
            response_stream = stub.Stream(self.generator())
            jpeg_buffer = b''
            try:
                for chunk in response_stream:
                    jpeg_buffer += chunk.frames

                    if jpeg_buffer.endswith(b'\xff\xd9'):
                        img_array = np.frombuffer(jpeg_buffer, dtype=np.uint8)
                        image = cv2.imdecode(img_array, cv2.IMREAD_COLOR)

                        if image is not None:
                            with open("/output/obraz.png", "wb") as f:
                                f.write(jpeg_buffer)
                        else:
                            print("Decoded image is None.")

                        jpeg_buffer = b''
                    
                    self.queue.append(chunk.frames)
            except grpc.RpcError as e:
                print(f"RpcError: {e.code()} - {e.details()}.")

if __name__ == '__main__':
    client = Client()

    threading.Thread(target=client.server_connection).start()

    time.sleep(5)
    client.stop = True