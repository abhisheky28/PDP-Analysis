# create_master_profile.py
# RUN THIS SCRIPT ONLY ONCE to create and log in to your master profile.

import time
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import os
import shutil

# This assumes config.py exists and has PROJECT_ROOT
import config

# --- Configuration ---
MASTER_PROFILE_PATH = os.path.join(config.PROJECT_ROOT, "Chrome-Master-Profile")

# --- Main Logic ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

    # Clean up any previous attempts
    if os.path.exists(MASTER_PROFILE_PATH):
        logging.info(f"Removing existing master profile at: {MASTER_PROFILE_PATH}")
        shutil.rmtree(MASTER_PROFILE_PATH)
        time.sleep(2)

    logging.info(f"Creating new master profile at: {MASTER_PROFILE_PATH}")
    
    options = Options()
    options.add_argument(f"--user-data-dir={MASTER_PROFILE_PATH}")
    options.add_argument("--no-first-run")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    print("\n" + "="*60)
    print("!!! ACTION REQUIRED !!!")
    print("A new Chrome browser has opened. You have 90 seconds.")
    print("1. Go to google.com")
    print("2. Sign in to your rankingautomation07@gmail.com account.")
    print("3. Accept any cookie pop-ups.")
    print("4. Close the browser MANUALLY when you are done.")
    print("The script will close automatically after 90 seconds.")
    print("="*60 + "\n")

    time.sleep(90)
    
    try:
        driver.quit()
    except:
        pass
        
    logging.info("Master profile has been created and primed. You can now run the main.py script.")