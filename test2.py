import subprocess
import numpy as np
import sounddevice as sd

samplerate = 44100
channels = 2
blocksize = 1024  # liczba próbek na blok
dtype = np.float32

# ffmpeg wypuszcza surowy float interleaved przez stdout
ffmpeg_cmd = [
    "ffmpeg",
    "-i", "video.mp4",
    "-f", "f32le",         # raw float32
    "-acodec", "pcm_f32le",
    "-ac", str(channels),
    "-ar", str(samplerate),
    "-loglevel", "quiet",
    "-"
]

# Uruchamiamy ffmpeg i czytamy z stdout
with subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE) as proc:
    with sd.OutputStream(
        samplerate=samplerate,
        channels=channels,
        dtype=dtype,
        blocksize=blocksize,
    ) as stream:
        while True:
            # Odczytaj blok (float32 × kanały × blocksize)
            raw = proc.stdout.read(blocksize * channels * 4)  # 4 bajty na float
            if not raw:
                break

            audio_block = np.frombuffer(raw, dtype=np.float32)

            audio_block = audio_block.reshape(-1, channels)

            stream.write(audio_block)
