import pyautogui
import pandas as pd
from auto_upload_module import *
import random

df = pd.read_csv('data_channel.csv')


channel = 'HP Crushing'


def change_channel(channel_name):
    #go to false url
    false_url = 'https://studio.youtube.com/channel/UCCrr7iOeJWUFOxLq2kdgYcg'
    access_yt_chanel(false_url)
    x,y = 880, 662
    print(f'move to {x, y}')
    pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)  
    pyautogui.click()
    result = df[df['Channel'] == channel_name].iloc[0]['pfp'].strip()
    location = pyautogui.locateOnScreen(result, confidence=0.5)
    if location:
        x, y = pyautogui.center(location)
        print(f'move to {x, y}')
        pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)  
        pyautogui.click()


change_channel('HP Crushing')
