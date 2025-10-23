# auto_runner.py
import time
import subprocess
import pandas as pd


while True:
    try:
        subprocess.run(["python", "csv_data\get_data.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
    time.sleep(30)

    ############ 1 ##############
    print("Running tuan_number.py ...")
    
    try:
        subprocess.run(["python", "tuan_number.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
    time.sleep(30)
    ############ 2 ##############

    print("Running tuan_tractor.py ...")
    try:
        subprocess.run(["python", "tuan_tractor.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
    time.sleep(30)
    
    ############ 3 ##############
    print("Running tuan_lolipop_asmr.py ...")
    try:
        subprocess.run(["python", "tuan_loli_pop.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
    time.sleep(30)
    
    ############ 4 ##############
    print("Running tuan_mini_toys_world.py ...")
    try:
        subprocess.run(["python", "tuan_mini_toys_world.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
    time.sleep(30)
    
    ############ 5 ##############
    print("Running tuan_thomas.py ...")
    try:
        subprocess.run(["python", "tuan_thomas.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
    time.sleep(30)

