FROM python:3.10
WORKDIR /streamer
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "-u", "main.py"]