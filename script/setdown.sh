rm -rf client/.venv
rm -rf server/.venv
rm -rf streamer/.venv
rm -rf protos/.venv
./script/pyclean.sh
./script/grpclean.sh
./script/clog.sh
./script/clegment.sh