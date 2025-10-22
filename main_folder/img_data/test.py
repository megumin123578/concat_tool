import pyautogui

# Tìm vị trí của nút "Upload" trên màn hình
location = pyautogui.locateOnScreen('select_directory.png', confidence=0.7)
if location:
    center = pyautogui.center(location)
    pyautogui.click(center)
else:
   	print("Không tìm thấy nút Upload!")
