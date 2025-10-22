import pyautogui
import webbrowser
import time
import os
import pyperclip
from pywinauto import Application
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import random
import traceback
from auto_upload_module import *


EXCEL_FILE = 'temp_upload.xlsx'
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
CREDS_FILE = r"C:\Users\Admin\Documents\main\Tuan_number\main_folder\sheet.json"  
SHEET_NAME = "Auto_concat_vids"


# Cấu hình PyAutoGUI
pyautogui.FAILSAFE = True  
pyautogui.PAUSE = 0.2


def main():
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
        gc = gspread.authorize(creds)
        copy_from_ggsheet_to_excel(gc, SHEET_NAME, EXCEL_FILE, 6)

        filtered_df, full_df = pre_process_data(EXCEL_FILE)
        

    except Exception as e:
        print(f"Error in main execution: {e}")
        return

    for idx, row in filtered_df.iterrows():
        title = row['Title']
        description = row['Description']
        output_dir = row['output directory']
        chanel = row['Channel']
    
        thumbnail = row['Thumbnail']
        for_kids = row['For kids']
        monetization = row['Monetization']
        publish_hour = row['Publish hour']
        publish_date = row['Publish date']

        print(f"Monetization: {monetization}")
        print(f"For kids: {for_kids}")
        try:
            #false url
            youtube_url = 'https://studio.youtube.com/channel/UCnZVD65a5zSsmrDwzs9uzEg'
            #handle chanel url
            if chanel == 'Show ASMR':
                tag_name = "@ChosoftSoftware"
                numbers_of_playist = 3
                num_lines = 10
                add_territories = 'India, VietNam'
                level = 3
            
            elif chanel == '6789 Satisfying':
                tag_name = "@6789Satisfying"
                numbers_of_playist = 5
                num_lines = 6
                add_territories = ""
                level = 1

            access_yt_chanel(youtube_url)
            

            folder, filename = split_dir(output_dir) #file mp4 location

            #go to channel select page
            x,y = random.uniform(836,945),random.uniform(648,676)
            print(f"Move to ({x}, {y})")
            pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)  
            pyautogui.click()
            random_delay()  

            x_channel, y_channel = select_channel(tag_name,23)
            #turn off dev tool and select right channel
            pyautogui.hotkey('f12')
            print(f"Move to ({x_channel}, {y_channel})")
            pyautogui.moveTo(x_channel, y_channel, duration=0.3, tween=pyautogui.easeInOutQuad)  
            pyautogui.click()
              
            random_delay(3,4)

            #go to studio
            x,y =1856,148
            print(f"Move to ({x}, {y})")
            pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)  
            pyautogui.click()
            random_delay()

            x,y =1720,395
            print(f"Move to ({x}, {y})")
            pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)  
            pyautogui.click()
            random_delay()  

            #go to create button
            create_button_x, create_button_y = 1797, 150 
            print(f"Move to ({create_button_x}, {create_button_y})")
            pyautogui.moveTo(create_button_x, create_button_y, duration=0.3, tween=pyautogui.easeInOutQuad)  # Giảm từ 1s xuống 0.3s
            pyautogui.click()
            random_delay()  

            upload_video_x, upload_video_y = 1790, 200  
            print(f"Move to ({upload_video_x}, {upload_video_y})")
            pyautogui.moveTo(upload_video_x, upload_video_y, duration=0.3, tween=pyautogui.easeInOutQuad)  
            pyautogui.click()
            random_delay()

            choose_vid_x, choose_vid_y = 960, 540
            print(f'Move to {choose_vid_x, choose_vid_y}')
            pyautogui.moveTo(choose_vid_x, choose_vid_y, duration=0.3, tween=pyautogui.easeInOutQuad)  
            pyautogui.click()
            random_delay()

            choose_file(folder, filename)


            #Upload step
            # Title
            select_x, select_y = 689,407
            print(f'Move to {select_x, select_y}')
            pyautogui.moveTo(select_x, select_y, duration=0.3, tween=pyautogui.easeInOutQuad)  # Giảm từ 0.5s xuống 0.2s
            pyautogui.click()
            random_delay()
            pyperclip.copy(title)
            pyautogui.hotkey("ctrl", "a")
            pyautogui.hotkey("ctrl", "v")

            # description
            if str(description) != 'nan':
                select_x, select_y = 688,652
                print(f'Move to {select_x, select_y}')
                pyautogui.moveTo(select_x, select_y, duration=0.3, tween=pyautogui.easeInOutQuad)  # Giảm từ 0.5s xuống 0.2s
                pyautogui.click()
                random_delay() 
                pyperclip.copy(description)
                pyautogui.hotkey("ctrl", "a")
                pyautogui.hotkey("ctrl", "v")
            
            #thumbnail
                num_lines = description.count('\n') + 1 #calculate lines if column is not empty

            print(f'co tat ca {num_lines} dong')
            #when no change in size
            select_x, select_y = 609,820
            if num_lines < 5:
                print(f'Move to {select_x, select_y}')
                pyautogui.moveTo(select_x, select_y, duration=0.3, tween=pyautogui.easeInOutQuad) 
                pyautogui.click()
                random_delay() 
            else:
                scroll_bar_x, scroll_bar_y = 1434, 458 
                distance = (num_lines - 5)*11
                print(f'Move to {scroll_bar_x, scroll_bar_y}')
                pyautogui.moveTo(scroll_bar_x, scroll_bar_y, duration=0.3, tween=pyautogui.easeInOutQuad) 
                pyautogui.mouseDown()
                print(f'Move to {scroll_bar_x, scroll_bar_y}')
                pyautogui.moveTo(scroll_bar_x, scroll_bar_y + distance, duration=0.3, tween=pyautogui.easeInOutQuad) 
                pyautogui.mouseUp()


                location = pyautogui.locateOnScreen("img_data/thumbnail_upload.png", confidence=0.7)
                if location:
                    x, y = pyautogui.center(location) 
                    pyautogui.moveTo(x,y)
                    pyautogui.click()

            #handling thumbnail directory
            if str(thumbnail) != 'nan':
                thumb_folder, thumb_filename = split_dir(thumbnail)
                
            else:
                thumb_filename = get_thumbnail_dir(folder)

            if thumb_filename != None:
                choose_file(folder, thumb_filename)    
            else:
                time.sleep(random.uniform(1,2))
                pyautogui.hotkey('alt','f4')

            # get to playlist
            print(f'Move to {scroll_bar_x, scroll_bar_y}')
            pyautogui.moveTo(scroll_bar_x, scroll_bar_y, duration=0.3, tween=pyautogui.easeInOutQuad) 
            pyautogui.mouseDown()
            
            time.sleep(1)
            print(f'Move to {scroll_bar_x, 1025 + distance}')
            pyautogui.moveTo(scroll_bar_x, 1025 + distance, duration=0.3, tween=pyautogui.easeInOutQuad) 
            pyautogui.mouseUp()
            
                #open playlist select
            x,y = 650,410
            print(f'Move to {x,y}')
            pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
            pyautogui.click()
                #check box
            x,y = 560,400
            for _ in range(numbers_of_playist):
                print(f'Move to {x,y}')
                time.sleep(random.uniform(1, 2))
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
                pyautogui.click()
                y+=32
                #done
            x,y = 881,742
            print(f'Move to {x,y}')
            pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
            pyautogui.click()

            #set for kids
            if for_kids == 'Yes':
                x,y = 540,692
                print(f'Move to {x,y}')
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
                pyautogui.click()
            else:
                x,y = 540,726
                print(f'Move to {x,y}')
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
                pyautogui.click()
            
            if level == 3:
                #move to monetization
                x,y = 1385,960
                print(f'Move to {x,y}')
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
                pyautogui.click()
                
                if len(add_territories) > 0:
                    x,y = 538,754
                    print(f'Move to {x,y}')
                    pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
                    pyautogui.click()

                    x,y = 722,865
                    print(f'Move to {x,y}')
                    pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
                    pyautogui.click()
                    pyperclip.copy(add_territories)
                    random_delay()
                    pyautogui.hotkey('ctrl','v')

                x,y = 1117,615
                print(f'Move to {x,y}')
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
                pyautogui.click()


                x,y = 1435,474
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)
                pyautogui.mouseDown()
                pyautogui.moveTo(x, y+400, duration=0.3, tween=pyautogui.easeInOutQuad) 
                pyautogui.mouseUp()
                if len(add_territories) >0:
                    x,y = 609,544
                    print(f'Move to {x,y}')
                    pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
                    pyautogui.click()

                x,y = 687,842
                print(f'Move to {x,y}')
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
                pyautogui.click()
                pyperclip.copy(title)
                pyautogui.hotkey('ctrl','v')


                x,y = 1376,946
                print(f'Move to {x,y}')
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
                pyautogui.click()
                time.sleep(random.uniform(1,2))
                pyautogui.click()
                random_delay()


                #ad suitability
                x, y =1437, 399
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)
                pyautogui.mouseDown()
                pyautogui.moveTo(x, y+800, duration=0.3, tween=pyautogui.easeInOutQuad) 
                pyautogui.mouseUp()
                time.sleep(random.uniform(1,2))

                location = pyautogui.locateOnScreen("img_data/confirm_content.png", confidence=0.7)
                if location:
                    x, y = pyautogui.center(location) 
                    pyautogui.moveTo(x,y)
                    pyautogui.click()
                random_delay()

                #send submit
                location = pyautogui.locateOnScreen("img_data/send_submit.png", confidence=0.7)
                if location:
                    x, y = pyautogui.center(location) 
                    pyautogui.moveTo(x,y)
                    pyautogui.click()
                random_delay()

                time.sleep(random.uniform(3,3.5))

                x,y = 1386,955
                print(f'Move to {x,y}')
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)
                random_delay()
                pyautogui.click()
                
                

                #video cho trẻ em
                if for_kids == 'Yes': #dont have endscreen 
                    x,y = 1386,955
                    print(f'Move to {x,y}')
                    pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)
                    random_delay()
                    pyautogui.click()
                    

                #video không cho trẻ em
                elif for_kids == 'No': # False 
                    #add endscreen
                    add_endscreen()
            if level == 1:
                if for_kids == 'No':
                    #add endscreen
                    x,y = 1381, 955
                    print(f'Move to {x,y}')
                    pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
                    pyautogui.click()
                    add_endscreen()
                x,y = 1390, 960
                print(f'Move to {x,y}')
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
                pyautogui.click()

            #next
            x,y = 1390, 960
            print(f'Move to {x,y}')
            pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
            pyautogui.click()
            random_delay(1,2)
            print(f'Move to {x,y}')
            pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad) 
            pyautogui.click()

            #public stattus = không công khai
            
            x,y = 591, 532
            print(f'Move to {x,y}')
            pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)
            pyautogui.click()

            

            if str(publish_hour) != 'nan':

                #scroll
                x,y = 1435,463
                print(f'Move to {x,y}')
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)
                pyautogui.mouseDown()
                print(f'Move to {x,y+300}')
                pyautogui.moveTo(x, y+300, duration=0.3, tween=pyautogui.easeInOutQuad)
                pyautogui.mouseUp()

                #schedule
                x,y = 755, 645
                print(f'Move to {x,y}') #date
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)
                pyautogui.click()
                
                x,y = 695, 580
                print(f'Move to {x,y}')
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)
                pyautogui.click()
                pyperclip.copy(convert_date(publish_date))
                pyautogui.hotkey('ctrl','a')
                pyautogui.hotkey('ctrl','v')
                pyautogui.hotkey('enter')

                x, y = 772, 574 #hour
                print(f'Move to {x,y}')
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)
                pyautogui.click()
                pyperclip.copy(publish_hour)
                pyautogui.hotkey('ctrl','a')
                pyautogui.hotkey('ctrl','v')
                pyautogui.hotkey('enter')
                #Done
                x,y = 1375,960
                print(f'Move to {x,y}')
                pyautogui.moveTo(x, y, duration=0.3, tween=pyautogui.easeInOutQuad)
                pyautogui.click()


                x,y = 1115,675
                print(f'Move to {x,y}')
                pyautogui.moveTo(x, y, duration=0.2, tween=pyautogui.easeInOutQuad)
                time.sleep(random.uniform(1,2))
                pyautogui.click()

            else:
                x,y = 1384,944
                print(f'Move to {x,y}')
                pyautogui.moveTo(x, y, duration=0.2, tween=pyautogui.easeInOutQuad)
                time.sleep(random.uniform(1,2))
                pyautogui.click()

                
        

        except Exception as e:
            print(f"Lỗi xảy ra: {e}")

            traceback.print_exc()

        #update_exxcel
        filtered_df.at[idx, 'status'] = 'Uploaded'

    #update sheet
    excel_to_sheet(EXCEL_FILE, SHEET_NAME,6)
        
if __name__ == "__main__":

    main()