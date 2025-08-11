rm -rf client/.venv
rm -rf server/.venv
rm -rf streamer/.venv
find streamer/resource -mindepth 1 ! -name 'sample.mp4' -exec rm -rf {} +
rm -rf streamer/segment
rm -rf protos/.venv
rm -rf .vscode/
rm -rf client/upload
./script/pyclean.sh
./script/grpclean.sh
./script/clog.sh
./script/clegment.sh