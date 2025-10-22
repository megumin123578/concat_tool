import pandas as pd

# Đường dẫn tới file CSV
csv_path = r"C:\Users\Admin\Documents\main\Tuan_number\csv_data\show_asmr_data.csv"  # hoặc đổi thành tên file em đang dùng

# Đọc CSV
df = pd.read_csv(csv_path)

# In toàn bộ nội dung (cả tiêu đề và từng dòng)
print("== Nội dung toàn bộ CSV ==")
for index, row in df.iterrows():
    print(f"Dòng {index}:")
    for col in df.columns:
        print(f"  {col}: {row[col]}")
