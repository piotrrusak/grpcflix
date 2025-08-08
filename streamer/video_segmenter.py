import os, subprocess, cv2, json, shutil

class VideoSegmenter():
    def __init__(self, input_file_path, output_file_path, output_dir_path, segment_duration):
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        self.output_dir_path = output_dir_path
        self.segment_duration = segment_duration
        cap = cv2.VideoCapture(input_file_path)
        self.width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        self.height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        self.fps = cap.get(cv2.CAP_PROP_FPS)
    
    def segment(self):

        if os.path.isdir(self.output_dir_path):
            for filename in os.listdir(self.output_dir_path):
                os.remove(os.path.join(self.output_dir_path, filename))
        else:
            os.makedirs(self.output_dir_path, exist_ok=True)

        output_pattern = os.path.join(self.output_dir_path, "segment_%06d.mp4")

        subprocess.run([
            "ffmpeg",
            "-i", self.input_file_path,
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-g", "60",
            "-force_key_frames", "expr:gte(t,n_forced*1)",
            "-f", "segment",
            "-segment_time", str(self.segment_duration),
            "-reset_timestamps", "1",
            output_pattern
        ])
        
        temp = []

        for file_path in sorted(os.listdir(self.output_dir_path)):
            cap = cv2.VideoCapture(os.path.join(self.output_dir_path, file_path))

            size = os.path.getsize(os.path.join(self.output_dir_path, file_path))
            
            cap.release()

            temp.append(size)

        temp.append([self.width, self.height, self.fps])

        with open(self.output_file_path, 'w') as f:            
            json.dump(temp, f, indent=4)

if __name__ == '__main__':
    filename = "video.mp4"
    video_segmenter = VideoSegmenter(f"resource/{filename}", f"segment/{filename.split(".")[0]}/info.json", f"segment/{filename.split(".")[0]}", 1)
    video_segmenter.segment()