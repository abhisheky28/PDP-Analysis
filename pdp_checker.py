# pdp_checker.py (Version 2 - Corrected)
# This script correctly finds the last tracking column, inserts a new one,
# and scrapes Google by simulating user actions, including clicking "Tools".

import time
import random
import logging
import os
import gspread
import traceback
import re
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException

from webdriver_manager.chrome import ChromeDriverManager
import smtplib
from email.mime.text import MIMEText


# --- SCRIPT-SPECIFIC CONFIGURATION (DO NOT EDIT config.py) ---
# All settings for this specific task are here.
PDP_SPREADSHEET_ID = '1MMiZkAm8I8jumzuYghTZFgCdqylZppEURq2ulT8aey8'
PDP_SHEET_NAME = 'PDPs'
PDP_QUERIES_COLUMN = 'D' # The column letter that contains the search queries.
PDP_START_ROW = 4        # CORRECTED: The first row to process.
PDP_END_ROW = 32         # The last row to process.
DATE_HEADER_ROW = 2      # The row where dates are stored.

# Delays for this script to avoid being blocked.
PDP_DELAY_CONFIG = {
    "typing": {"min": 0.05, "max": 0.15},
    "after_page_load": {"min": 3, "max": 5},
    "between_queries": {"min": 7, "max": 15}
}

# --- SHARED CONFIGURATION ---
# We import the original config file to use its shared settings
# like credentials path, email settings, user agents, and CAPTCHA timeouts.
try:
    import config
except ImportError:
    print("FATAL ERROR: config.py not found. Make sure it is in the same directory.")
    exit()

# --- LOGGING SETUP ---
log_file_path = os.path.join(config.PROJECT_ROOT, 'pdp_checker.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, mode='w'),
        logging.StreamHandler()
    ]
)

# --- SELECTORS FOR SCRAPING ---
SEARCH_INPUT_SELECTOR = "[name='q']"
TOOLS_BUTTON_SELECTOR = '#hdtb-tls'
RESULT_STATS_SELECTOR = '#result-stats'
NO_RESULTS_TEXT = 'No results found for'

# --- HELPER FUNCTIONS (ADAPTED FROM YOUR EXISTING PROJECT) ---

def send_error_email(subject, body):
    if not config.ENABLE_EMAIL_NOTIFICATIONS: return
    recipients = config.RECIPIENT_EMAIL
    logging.info(f"Preparing to send error email to: {', '.join(recipients)}")
    try:
        msg = MIMEText(body, 'plain')
        msg['Subject'], msg['From'], msg['To'] = subject, config.SENDER_EMAIL, ", ".join(recipients)
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            server.starttls(); server.login(config.SENDER_EMAIL, config.SENDER_PASSWORD)
            server.sendmail(config.SENDER_EMAIL, recipients, msg.as_string())
            logging.info("Error email sent successfully.")
    except Exception as e:
        logging.error(f"CRITICAL: FAILED TO SEND ERROR EMAIL. Error: {e}")

def handle_captcha(driver, keyword):
    alert_sent = False; start_time = time.time()
    logging.warning("!!! CAPTCHA DETECTED !!! Pausing script and waiting for manual intervention.")
    while time.time() - start_time < config.CAPTCHA_WAIT_TIMEOUT:
        if "reCAPTCHA" not in driver.page_source and "unusual traffic" not in driver.page_source:
            logging.info("CAPTCHA appears to be solved! Resuming script."); return True
        if not alert_sent:
            print(f"\n{'='*60}\nACTION REQUIRED: Please solve the CAPTCHA in the browser.\n"
                  f"The script will wait for up to {config.CAPTCHA_WAIT_TIMEOUT / 60:.0f} minutes.\n{'='*60}\n")
            send_error_email("PDP Checker Alert: CAPTCHA - Action Required",
                             f"Hello,\n\nThe PDP Checker script has encountered a Google CAPTCHA.\n\nKeyword: \"{keyword}\"\n\nPlease solve the security check in the browser. The script will automatically resume.\n\n- Automated System")
            alert_sent = True
        time.sleep(config.CAPTCHA_CHECK_INTERVAL); print(".", end="", flush=True)
    logging.error(f"CAPTCHA Timeout! Waited for {config.CAPTCHA_WAIT_TIMEOUT} seconds."); return False

def connect_to_google_sheets():
    logging.info("Connecting to Google Sheets API...");
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(config.GCP_CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds); logging.info("Successfully connected to Google Sheets API.")
    return client

def get_webdriver():
    logging.info("Initializing non-headless Chrome WebDriver...")
    options = webdriver.ChromeOptions()
    options.add_argument(f"--user-data-dir={config.CHROME_PROFILE_PATH}")
    options.add_argument(f'user-agent={random.choice(config.USER_AGENTS)}')
    options.add_argument("--start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    service = Service(ChromeDriverManager().install()); driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60); return driver

def human_like_typing(element, text):
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(PDP_DELAY_CONFIG["typing"]["min"], PDP_DELAY_CONFIG["typing"]["max"]))

def find_and_type_in_search_box(driver, text):
    try:
        search_box = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, SEARCH_INPUT_SELECTOR)))
        search_box.clear(); human_like_typing(search_box, text); search_box.send_keys(Keys.RETURN)
        return True
    except TimeoutException:
        logging.error("Could not find the search box. Cannot perform search."); return False

def prepare_sheet_and_get_target_column(worksheet):
    logging.info(f"Preparing sheet '{worksheet.title}'...")
    try:
        date_row_values = worksheet.row_values(DATE_HEADER_ROW)
        last_tracking_col_index = 0
        # Find the last column that contains a date-like value (e.g., "DD-Mon-YY" or "DD/MM/YYYY")
        for i, cell_value in enumerate(date_row_values):
            if re.search(r'\d', cell_value) and ('-' in cell_value or '/' in cell_value):
                last_tracking_col_index = i + 1
        
        if last_tracking_col_index == 0:
            raise Exception("Could not find any date-like headers in row 2 to determine where to insert the new column.")

        new_col_index = last_tracking_col_index + 1
        logging.info(f"Last tracking data found in column {last_tracking_col_index}. Inserting new column at index {new_col_index}.")
        worksheet.insert_cols([[]], col=new_col_index, inherit_from_before=True)
        logging.info("Successfully inserted new column.")

        today_date = datetime.now().strftime('%d-%b-%Y') # e.g., 25-May-2024
        worksheet.update_cell(DATE_HEADER_ROW, new_col_index, today_date)
        logging.info(f"Wrote today's date '{today_date}' to row {DATE_HEADER_ROW}, column {new_col_index}.")
        return new_col_index
    except Exception as e:
        logging.error(f"Failed to prepare the sheet: {e}"); raise

def scrape_result_count(driver):
    try:
        # 1. Click the "Tools" button to reveal the result stats
        tools_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, TOOLS_BUTTON_SELECTOR)))
        tools_button.click()
        logging.info("Clicked 'Tools' button.")
        time.sleep(1) # Short pause for the UI to update

        # 2. Now that it's visible, scrape the result stats
        stats_element = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, RESULT_STATS_SELECTOR)))
        stats_text = stats_element.text
        
        match = re.search(r'([\d,]+) result', stats_text, re.IGNORECASE)
        if match and match.group(1):
            return match.group(1) # Returns "71,60,000"
            
    except (TimeoutException, ElementClickInterceptedException):
        if NO_RESULTS_TEXT in driver.page_source:
            logging.warning("Page indicates 'No results found'."); return '0'
        else:
            logging.error("Failed to click 'Tools' or find result stats. Possible CAPTCHA or layout change."); return "Scrape Failed"
    except Exception as e:
        logging.error(f"An unexpected error occurred during scraping: {e}"); return "Scrape Failed"

def main():
    logging.info("--- Starting PDP Sheet Automator Script (V2) ---")
    driver = None
    try:
        gspread_client = connect_to_google_sheets()
        spreadsheet = gspread_client.open_by_key(PDP_SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(PDP_SHEET_NAME)
        
        target_col = prepare_sheet_and_get_target_column(worksheet)

        query_range = f"{PDP_QUERIES_COLUMN}{PDP_START_ROW}:{PDP_QUERIES_COLUMN}{PDP_END_ROW}"
        logging.info(f"Fetching queries from range: {query_range}")
        queries_data = worksheet.get(query_range)
        queries = [item[0] for item in queries_data if item]
        logging.info(f"Found {len(queries)} queries to process.")

        driver = get_webdriver()

        for i, query in enumerate(queries):
            current_row = PDP_START_ROW + i
            logging.info(f"\n--- Processing query {i+1}/{len(queries)}: '{query}' for row {current_row} ---")

            if not query.strip():
                logging.warning(f"Skipping empty query at row {current_row}.")
                result_count = ""
            else:
                driver.get("https://www.google.com")
                time.sleep(random.uniform(1, 2))
                if not find_and_type_in_search_box(driver, query):
                    continue # Skip if search fails
                
                time.sleep(random.uniform(PDP_DELAY_CONFIG["after_page_load"]["min"], PDP_DELAY_CONFIG["after_page_load"]["max"]))

                if "unusual traffic" in driver.page_source or "reCAPTCHA" in driver.page_source:
                    if not handle_captcha(driver, query):
                        result_count = "CAPTCHA FAILED"
                    else: # Re-scrape after solving
                        result_count = scrape_result_count(driver)
                else:
                    result_count = scrape_result_count(driver)

            logging.info(f"Scraped result: '{result_count}'")
            
            # Update the sheet immediately for this single row
            worksheet.update_cell(current_row, target_col, str(result_count))
            logging.info(f"SUCCESS: Updated cell in row {current_row}, column {target_col}.")

            time.sleep(random.uniform(PDP_DELAY_CONFIG["between_queries"]["min"], PDP_DELAY_CONFIG["between_queries"]["max"]))

    except Exception as e:
        error_traceback = traceback.format_exc()
        logging.critical(f"A critical, unhandled error occurred: {e}\n{error_traceback}")
        send_error_email("PDP Checker Alert: SCRIPT CRASHED", f"The PDP Checker script has crashed.\n\nError:\n{e}\n\nTraceback:\n{error_traceback}")

    finally:
        if driver:
            logging.info("Closing WebDriver."); driver.quit()
        logging.info("--- PDP Sheet Automator Script Finished ---")

if __name__ == "__main__":
    main()