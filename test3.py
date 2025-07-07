import subprocess
import json

def get_video_info(filepath):
    command = [
        'ffprobe',
        '-v', 'quiet',
        '-print_format', 'json',
        '-show_format',
        '-show_streams',
        filepath
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return json.loads(result.stdout)

video_info = [
    "width",
    "height",
    "r_frame_rate",
    "duration",
    "nb_frames",
]

audio_info = [
    "codec_name",
    "channels",
    "sample_rate",
    "sample_fmt",
    "channel_layout",
]

for info in video_info:
    print(info, ": ", get_video_info('video.mp4')["streams"][0][info])

for info in audio_info:
    print(info, ": ", get_video_info('video.mp4')["streams"][1][info])


# info = get_video_info('video.mp4')
# print(json.dumps(info, indent=2))