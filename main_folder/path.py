import os
from pathlib import Path

def get_filename_without_extension(file_path):
    base_name = os.path.basename(file_path)              # Lấy tên file: '316KLG.mp4'
    name_without_ext = os.path.splitext(base_name)[0]    # Bỏ đuôi: '316KLG'
    return name_without_ext

# Use double backslashes to escape
path = "H:\\Number\\Number B 2.0\\316KLG\\316KLG.mp4"
print(get_filename_without_extension(path))
