# refresh_profile.py
# RUN THIS SCRIPT whenever you feel the master profile is stale or logged out.
# It will delete the old profile and guide you through creating a new one.

import time
import logging
import os
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# This assumes config.py exists and has PROJECT_ROOT and CHROME_PROFILE_PATH
import config

# --- Main Logic ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

    # Use the path from the central config file
    MASTER_PROFILE_PATH = config.CHROME_PROFILE_PATH

    # 1. Clean up any previous profile
    if os.path.exists(MASTER_PROFILE_PATH):
        logging.info(f"Removing existing master profile at: {MASTER_PROFILE_PATH}")
        try:
            shutil.rmtree(MASTER_PROFILE_PATH)
            logging.info("Successfully removed old profile.")
            time.sleep(2) # Give the OS a moment to process the deletion
        except OSError as e:
            logging.error(f"Could not remove profile. Is a Chrome window using it still open? Error: {e}")
            logging.error("Please close all Chrome windows and run this script again.")
            exit() # Stop the script if we can't delete the folder

    # 2. Launch the creation process (logic from create_master_profile.py)
    logging.info(f"Creating new master profile at: {MASTER_PROFILE_PATH}")
    
    options = Options()
    options.add_argument(f"--user-data-dir={MASTER_PROFILE_PATH}")
    options.add_argument("--no-first-run")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    print("\n" + "="*60)
    print("!!! ACTION REQUIRED: PROFILE REFRESH !!!")
    print("A new Chrome browser has opened. You have 120 seconds.")
    print("1. Go to google.com")
    print("2. Sign in to your rankingautomation07@gmail.com account.")
    print("3. IMPORTANT: If Google asks 'Turn on sync?', click 'No thanks'.")
    print("4. Accept any cookie pop-ups.")
    print("5. Close the browser MANUALLY when you are done.")
    print("The script will close automatically after 120 seconds.")
    print("="*60 + "\n")

    time.sleep(120) # Increased time to 2 minutes
    
    try:
        driver.quit()
    except:
        pass
        
    logging.info("Master profile has been refreshed. You can now run the main.py or ranking_automator.py script.")