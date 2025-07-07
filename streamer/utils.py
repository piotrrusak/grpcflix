import subprocess
import json
import ffmpeg

def get_all_mp4_info(filepath):
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

def get_video_info(filepath, video_info):
    temp = dict()
    for info in video_info:
        temp[info] = get_all_mp4_info(filepath)["streams"][0][info]
    return temp

def get_audio_info(filepath, audio_info):
    temp = dict()
    for info in audio_info:
        temp[info] = get_all_mp4_info(filepath)["streams"][1][info]
    return temp

def convert_video_to_bytes(filepath):
    process = (
        ffmpeg
            .input("video.mp4")
            .output('pipe:', format='rawvideo', pix_fmt='rgb24')
            .run_async(pipe_stdout=True)
    )
    return process.stdout.read()

if __name__ == "__main__":

    filepath = 'video.mp4'
    
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

    print(get_video_info(filepath, video_info))
    print(get_audio_info(filepath, audio_info))

    # print(json.dumps(get_all_mp4_info(filepath), indent=2))