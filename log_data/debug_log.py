from pymediainfo import MediaInfo
import pandas as pd
from pathlib import Path

def extract_mediainfo(video_path):
    info = {
        "path": str(video_path),
        "duration": "",
        "bit_rate": "",
        "frame_rate": "",
        "resolution": "",
        "format": "",
        "format_profile": "",
        "codec": "",
        "scan_type": "",
        "bit_depth": "",
        "chroma_subsampling": "",
        "aspect_ratio": "",
        "video_size_MB": "",
        "audio_codec": "",
        "audio_bitrate": "",
        "audio_sampling_rate": "",
        "channels": "",
    }

    media_info = MediaInfo.parse(video_path)
    for track in media_info.tracks:
        if track.track_type == "General":
            info["format"] = track.format
            info["format_profile"] = track.format_profile or ""
            info["duration"] = track.other_duration[0] if track.other_duration else ""
            info["bit_rate"] = track.other_overall_bit_rate[0] if track.other_overall_bit_rate else ""
            info["video_size_MB"] = track.other_file_size[0] if track.other_file_size else ""
        elif track.track_type == "Video":
            info["codec"] = track.codec_id
            info["resolution"] = f"{track.width}x{track.height}"
            info["aspect_ratio"] = track.display_aspect_ratio
            info["frame_rate"] = track.frame_rate
            info["scan_type"] = track.scan_type
            info["bit_depth"] = f"{track.bit_depth} bits" if track.bit_depth else ""
            info["chroma_subsampling"] = track.chroma_subsampling
        elif track.track_type == "Audio":
            info["audio_codec"] = track.codec_id
            info["audio_bitrate"] = track.other_bit_rate[0] if track.other_bit_rate else ""
            info["audio_sampling_rate"] = track.other_sampling_rate[0] if track.other_sampling_rate else ""
            info["channels"] = track.channel_s

    return info

def process_log_file(log_file_path, output_csv="video_full_info.csv"):
    log_file = Path(log_file_path)
    if not log_file.exists():
        print(f"Không tìm thấy file log: {log_file}")
        return

    results = []
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            path = line.strip()
            if path and Path(path).exists():
                print(f"Đang xử lý: {path}")
                try:
                    info = extract_mediainfo(path)
                    results.append(info)
                except Exception as e:
                    results.append({"path": path, "error": str(e)})
            else:
                results.append({"path": path, "error": "File không tồn tại"})

    df = pd.DataFrame(results)
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"Đã lưu vào: {output_csv}")

if __name__ == "__main__":
    process_log_file("show_asmr_used.log")
