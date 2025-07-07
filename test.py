import ffmpeg
import numpy as np
import cv2
width, height = 1080, 1080  # ustaw zgodnie z wideo
import time

process = (
    ffmpeg
    .input("video.mp4")
    .output('pipe:', format='rawvideo', pix_fmt='rgb24')
    .run_async(pipe_stdout=True)
)

time.sleep(1)

print("type: ", type(process.stdout.read(width * height * 3)))



while True:
    in_bytes = process.stdout.read(width * height * 3)
    if not in_bytes:
        break

    frame = (
        np
        .frombuffer(in_bytes, np.uint8)
        .reshape([height, width, 3])
    )

    print(type(frame))

    cv2.imshow("Klatka", frame)
    if cv2.waitKey(int(1000/60)) & 0xFF == ord('q'):
        break

process.stdout.close()
cv2.destroyAllWindows()
