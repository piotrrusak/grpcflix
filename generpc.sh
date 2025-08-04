source protos/.venv/bin/activate
./grpclean.sh
cd protos/client_server
python3 -m grpc_tools.protoc -I./ --python_out=. --grpc_python_out=. client_server.proto
cd ..
cd ..
cp protos/client_server/client_server_pb2.py client/
cp protos/client_server/client_server_pb2.py server/
cp protos/client_server/client_server_pb2_grpc.py client/
cp protos/client_server/client_server_pb2_grpc.py server/
cd protos/server_streamer
python3 -m grpc_tools.protoc -I./ --python_out=. --grpc_python_out=. server_streamer.proto
cd ..
cd ..
cp protos/server_streamer/server_streamer_pb2.py server/
cp protos/server_streamer/server_streamer_pb2.py streamer/
cp protos/server_streamer/server_streamer_pb2_grpc.py server/
cp protos/server_streamer/server_streamer_pb2_grpc.py streamer/
deactivate