import os

folder = input('Enter folder name: ').strip()
output_txt = r'log_data\music\music_list.txt'

os.makedirs(os.path.dirname(output_txt), exist_ok=True)
mp3_files = set() 
for root, dirs, files in os.walk(folder):
    for f in files:
        if f.lower().endswith(".mp3"):
            mp3_files.add(f)   


with open(output_txt, "w", encoding="utf-8") as txt:
    for name in sorted(mp3_files): 
        name = name.replace('.mp3','')
        txt.write(name + "\n")

print("Đã lưu:", output_txt)
