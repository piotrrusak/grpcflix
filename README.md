# grpcflix

Prototype video streaming service using gRPC and Python – live chat and user-uploaded videos included.

## scripts

- **clegment.sh** deletes all files in streamer/segment/
- **clog.sh** deletes all .log files
- **generpc.sh** generates all grpc files and copies them to their destination
- **grpclean.sh** deletes all grpc files (apart from .proto)
- **pyclean.sh** deletes all __pycache__
- **setup.sh** sets up project
- **setdown.sh** sets down project

# conclusions

- **Qt/cv2:** library for GUI in linux, requires that we open window and manipulate it, in main thread of process. So in cv2 requires it as well. Best practise is to just use cv2 in main thread of process of programm. In here i just use multiprocessing instead of threading.

---

- **logging:** library for logging in python. very useful. Quick tutorial:

```
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
logger.propagate = False```

```

---

- **grpc:** when changing .proto file, by careful about named arguments, and names of objects.

---

- **first GIL problem** projection with pygame oveloads GIL, because of this other threads has no possibility to run, because of this, projection blocked server_connection in client.

---

- **cv2 flags** Used by VideoCapture.set(flag, value): https://docs.opencv.org/3.4/d4/d15/group__videoio__flags__base.html#gaeb8dd9c89c10a5c63c139bf7c4f5704d

# TODO

## Short term

- Cleanup code after developing upload.

## Long term

- Chat

- Better projection than now.

- Audio