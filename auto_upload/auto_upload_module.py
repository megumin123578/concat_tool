import os
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path
import time
import pyautogui
import random
import pyperclip
import webbrowser
from bs4 import BeautifulSoup

def excel_to_sheet(excel_file, sheet_file, worksheet_index):
    df = pd.read_excel(excel_file, engine="openpyxl")

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    CREDS_FILE = "sheet.json"

    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)

    spreadsheet = gc.open(sheet_file)

    try:
        worksheet = spreadsheet.get_worksheet(worksheet_index)  # Lấy theo index
    except Exception as e:
        print(f"Không thể lấy worksheet tại index {worksheet_index}: {e}")
        return

    worksheet.clear()

    data = [df.columns.tolist()] + df.fillna('').astype(str).values.tolist()
    worksheet.update("A1", data)

    print(f"Đã ghi nội dung vào worksheet index {worksheet_index} trong '{sheet_file}'.")


def get_thumbnail_dir(folder_path):
    if not os.path.exists(folder_path):
        print(f"Thư mục {folder_path} không tồn tại!")
        return None

    image_extensions = ('.png', '.jpg', '.jpeg')

    image_files = [f for f in os.listdir(folder_path) if f.lower().endswith(image_extensions)]

    if len(image_files) == 1:
        return image_files[0]  
    else:
        print(f"Thư mục không chứa file ảnh (.png, .jpg, .jpeg).")
        return None
def random_delay(min_sec = 0.5, max_sec = 1):
    time.sleep(random.uniform(min_sec, max_sec))

def off_set_(x, y, delta=5):
    rand_x = x + random.randint(-delta, delta)
    rand_y = y + random.randint(-delta, delta)
    pyautogui.moveTo(rand_x, rand_y, duration=random.uniform(0.2, 0.5))
    pyautogui.click()


def convert_date(input_date):
    date_splited = input_date.split('/')

    pl_date = f'{date_splited[0]} thg {date_splited[1]}, 20{date_splited[2]}'

    return pl_date

def clear_excel_file(excel_file):
    try:
        columns = ['first vids', 'desired length', 'output directory', 'number_of_vids', 'status']
        empty_df = pd.DataFrame(columns=columns)
        empty_df.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"Cleared existing content in Excel file: {excel_file}")
    except Exception as e:
        print(f"Error clearing Excel file '{excel_file}': {e}")





def pre_process_data(file):
    df = pd.read_excel(file)
    filtered_df = df[
        df['Channel'].notna() &
        df['output directory'].notna() &
        df['status'].str.lower().eq('upload')
    ]
    return filtered_df, df
def copy_from_ggsheet_to_excel(gspread_client, sheet_name, excel_file, idx):
    try:
        spreadsheet = gspread_client.open(sheet_name)
        worksheet = spreadsheet.get_worksheet(idx)
        data = worksheet.get_all_values()

        if not data:
            print("Google Sheet is empty!")
            return
        
        columns = data[0]  
        values = data[1:]  
        df = pd.DataFrame(values, columns=columns)
        clear_excel_file(excel_file)
        df.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"Successfully copied data from Google {sheet_name} to Excel file {excel_file}")
    except Exception as e:
        print(f"Error copying data from Google Sheet to Excel: {e}")

def access_yt_chanel(url):
    print(f"Mở trình duyệt và truy cập: {url}")
    webbrowser.open(url)
    time.sleep(4.5)  

def split_dir(dir):
    #split_dir folder and filename
    folder = os.path.dirname(dir)
    filename = os.path.basename(dir)

    return folder, filename


def choose_file(folder_dir, file_name):
        time.sleep(1)
        ###############################CHOOSE FILE#######################################
        location = pyautogui.locateOnScreen("img_data/tham_chieu.png", confidence=0.8)
        if location:
            x, y = pyautogui.center(location) 
            x += 1123
            y -= 737
            pyautogui.moveTo(x,y)
            pyautogui.click()
        random_delay()
        #enter folder
        pyperclip.copy(folder_dir)
        pyautogui.hotkey("ctrl", "v")
        pyautogui.hotkey("enter")

        location = pyautogui.locateOnScreen("img_data/tham_chieu.png", confidence=0.7)
        if location:
            x, y = pyautogui.center(location) 
            x += 1371
            y -= 737
            pyautogui.moveTo(x,y)
            pyautogui.click()

        random_delay()
        #enter filename
        pyperclip.copy(file_name)
        pyautogui.hotkey("ctrl", "v")
        pyautogui.hotkey("enter")
        random_delay(1,2)

        #select file
        select_x, select_y = x-1243, y+140
        print(f'Move to {select_x, select_y}')
        pyautogui.moveTo(select_x, select_y, duration=0.3, tween=pyautogui.easeInOutQuad)  
        pyautogui.click()
        random_delay()

        pyautogui.hotkey('enter')
        time.sleep(5)
        ##########################################################################
    

def get_tag_name(html):
    soup = BeautifulSoup(html, 'html.parser')
    tag = soup.find('yt-formatted-string')
    if tag:
        return tag.text.strip()
    return None

def select_channel(channel_tag, total_channel):
    #go to def tool

    time.sleep(2)
    first_channel = [453,285]
    x,y = first_channel[0], first_channel[1]
    

    for _ in range(total_channel):

        x += 300 #move x to seccond col
        pyautogui.hotkey('ctrl','shift','c')
        random_delay(1,2 )
        print(f"Move to ({x}, {y})")
        pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)  
        pyautogui.click()
        random_delay() 
        pyautogui.hotkey('ctrl','c')
        time.sleep(1)
        tag_name = get_tag_name(pyperclip.paste())
        print(tag_name)
        if tag_name == channel_tag:
            return x +275,y
        
        x += 300 #move x to third col
        pyautogui.hotkey('ctrl','shift','c')
        random_delay(1,2) 
        print(f"Move to ({x}, {y})")
        pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)  
        pyautogui.click()
        random_delay() 
        pyautogui.hotkey('ctrl','c')
        time.sleep(1)
        tag_name = get_tag_name(pyperclip.paste())
        print(tag_name)
        if tag_name == channel_tag:
            return x+275,y

        x -= 600 #move x to first col
        y+= 65  #move y to next row
        pyautogui.hotkey('ctrl','shift','c')
        random_delay(1,2) 
        print(f"Move to ({x}, {y})")
        pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)  
        pyautogui.click()
        random_delay() 
        pyautogui.hotkey('ctrl','c')
        time.sleep(1)
        tag_name = get_tag_name(pyperclip.paste())
        print(tag_name)
        if tag_name == channel_tag:
            return x+275,y
        
def add_endscreen():
    x,y = 1345,545
    print(f'Move to {x,y}')
    pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
    pyautogui.click()
    
    time.sleep(2)

    x,y = 625,385
    print(f'Move to {x,y}')
    pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
    pyautogui.click()

    #modify
    x,y = 1400, 790
    pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)
    pyautogui.mouseDown()
    pyautogui.moveTo(x+50, y, duration=0.3, tween=pyautogui.easeInOutQuad)
    pyautogui.mouseUp()

    x,y = 1400, 820
    pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)
    pyautogui.mouseDown()
    pyautogui.moveTo(x+50, y, duration=0.3, tween=pyautogui.easeInOutQuad)
    pyautogui.mouseUp()

    #save
    x,y = 1385,230
    print(f'Move to {x,y}')
    pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
    pyautogui.click()
    time.sleep(1)