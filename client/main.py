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
        time.sleep(4)

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
        current = 0
        print("server_connection")
        with grpc.insecure_channel('server:50001') as channel:
            stub = video_pb2_grpc.VideoServiceStub(channel)
            response_stream = stub.Stream(self.generator())
            buffer = b''
            try:
                for chunk in response_stream:
                    buffer += chunk.frames
                    if len(buffer) > (1080 * 1080 * 3) * (current+1):
                        frame_bytes = buffer[1080*1080*3*current:1080*1080*3 * (current+1)]
                        frame = np.frombuffer(frame_bytes, dtype=np.uint8).reshape((1080, 1080, 3))
                        print(time.time())
                        cv2.imshow("Klatka", frame)
                        print("ciul")
                        if cv2.waitKey(int(1000/60)) & 0xFF == ord('q'):
                            break
                            
                        # buffer = b''
                    
                    self.queue.append(chunk.frames)
            except grpc.RpcError as e:
                print(f"RpcError: {e.code()} - {e.details()}.")

if __name__ == '__main__':
    client = Client()

    threading.Thread(target=client.server_connection).start()

    time.sleep(5)
    client.stop = True