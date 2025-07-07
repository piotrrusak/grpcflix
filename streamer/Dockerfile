FROM python:3.10
WORKDIR /streamer
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y ffmpeg
CMD ["python", "-u", "main.py"]