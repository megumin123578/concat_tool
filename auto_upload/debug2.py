import pytesseract
import pyautogui
import cv2
import numpy as np
import re
from PIL import Image


location = pyautogui.locateOnScreen("img_data/tham_chieu.png", confidence=0.8)
if location:
    x, y = pyautogui.center(location) 

    pyautogui.moveTo(x,y)
    print(x,y)
