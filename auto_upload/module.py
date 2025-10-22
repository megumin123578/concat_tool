import pyautogui
import pyperclip
import time
import random
import webbrowser

from bs4 import BeautifulSoup

def get_tag_name(html):
    soup = BeautifulSoup(html, 'html.parser')
    tag = soup.find('yt-formatted-string')
    if tag:
        return tag.text.strip()
    return None

def access_yt_chanel(url):
    print(f"Mở trình duyệt và truy cập: {url}")
    webbrowser.open(url)
    time.sleep(4.5)  

def random_delay(min_sec = 0.5, max_sec = 1):
    time.sleep(random.uniform(min_sec, max_sec))

def select_channel(channel_tag, total_channel):
    #go to def tool

    time.sleep(2)
    first_channel = [453,285]
    x,y = first_channel[0], first_channel[1]
    

    for _ in range(total_channel):

        x += 300 #move x to seccond col
        pyautogui.hotkey('ctrl','shift','c')
        print(f"Move to ({x}, {y})")
        pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)  
        pyautogui.click()
        random_delay() 
        pyautogui.hotkey('ctrl','c')
        time.sleep(1)
        tag_name = get_tag_name(pyperclip.paste())
        print(tag_name)
        if tag_name == channel_tag:
            return x,y
        
        x += 300 #move x to third col
        pyautogui.hotkey('ctrl','shift','c')
        print(f"Move to ({x}, {y})")
        pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)  
        pyautogui.click()
        random_delay() 
        pyautogui.hotkey('ctrl','c')
        time.sleep(1)
        tag_name = get_tag_name(pyperclip.paste())
        print(tag_name)
        if tag_name == channel_tag:
            return x,y

        x -= 600 #move x to first col
        y+= 65  #move y to next row
        pyautogui.hotkey('ctrl','shift','c')
        print(f"Move to ({x}, {y})")
        pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)  
        pyautogui.click()
        random_delay() 
        pyautogui.hotkey('ctrl','c')
        time.sleep(1)
        tag_name = get_tag_name(pyperclip.paste())
        print(tag_name)
        if tag_name == channel_tag:
            return x,y


access_yt_chanel('https://studio.youtube.com/channel/UCCrr7iOeJWUFOxLq2kdgYcg')
x,y =random.uniform(836,945),random.uniform(648,676)
print(f"Move to ({x}, {y})")
pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)  
pyautogui.click()
random_delay()  
print(select_channel('@SweetCaking',23))