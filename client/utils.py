import ffmpeg
import numpy as np
import cv2

def display_video_from_buffer(buffer, width, height):
    process = ffmpeg.input("video.mp4").output("pipe: ", format="rawvideo", pix_fmt="rgb24").run_async(pipe_stdout=True)
    print(process.stedout.read(width * height * 3))
    while True:
        batch = process.stdout.read(width * height * 3)
        frame = np.frombuffer(batch, np.uint8).reshape([width, height, 3])

        cv2.imshow(frame)
        if cv2.waitkey(int(1000/60)) & 0xFF == ord('q'):
            break
