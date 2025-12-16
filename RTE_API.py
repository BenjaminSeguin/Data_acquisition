import os
import time
import glob
import zipfile
import argparse
from datetime import datetime, timedelta  # <--- Add this import
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

class RTEAPI:
    def __init__(self, download_folder):
        """
        Initialize with a specific download folder.
        """
        self.download_folder = os.path.abspath(download_folder)
        self.driver = None

    def _setup_driver(self):
        """Configure Chrome with custom download settings."""
        options = Options()
        prefs = {
            "download.default_directory": self.download_folder,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)

        '''options.add_argument("--no-sandbox") 
        options.add_argument("--disable-dev-shm-usage") 
        options.add_argument("--disable-gpu")'''
        
        # Initialize Chrome
        self.driver = webdriver.Chrome(options=options) #standard webscrapping browser

    def open_page(self):
        """
        Opens the specified URL using the Selenium driver.
        """
        url = "https://www.rte-france.com/donnees-publications/eco2mix-donnees-temps-reel/telecharger-indicateurs"

        if not self.driver:
            self._setup_driver()
        self.driver.get(url)

    def close_page(self):
        """
        Closes the Selenium driver.
        """
        if self.driver:
            self.driver.quit()
            self.driver = None

    def download_data(self, startdate, enddate, final_filename=None):
        """
        Navigates to RTE, fills the specific 'date' ID, and clicks 'dlbtn'.
        """
        # ------- Check if file already exists -------
        if final_filename:
            full_target_path = os.path.join(self.download_folder, final_filename)
            if os.path.exists(full_target_path):
                print(f"File '{final_filename}' already exists. Skipping download.")
                return # Exit the function immediately
        # ---------------------------------------

        if not self.driver:
            self._setup_driver()

        current_date = datetime.strptime(startdate, "%d/%m/%Y")
        if enddate is None:
            end_date = current_date
        else:
            end_date = datetime.strptime(enddate, "%d/%m/%Y")

        while current_date <= end_date:
            try:
                '''# Handle Cookie Popup (Standard practice)
                try:
                    cookie_btn = wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
                    cookie_btn.click()
                    print("Cookies accepted.")
                    time.sleep(1) 
                except:
                    print("No cookie popup found (or already accepted).")'''
                
                # Locate the Date Input
                wait = WebDriverWait(self.driver, 15)
                date_input = wait.until(EC.visibility_of_element_located((By.ID, "date")))

                # Clear and Enter Date (Human Method)
                date_input.click()
                time.sleep(0.2)

                for _ in range(10):
                    date_input.send_keys(Keys.BACKSPACE)
                    date_input.send_keys(Keys.DELETE)
                    time.sleep(0.02)

                time.sleep(0.02)
                date_input.send_keys(current_date.strftime("%d/%m/%Y"))
                time.sleep(0.02)
                date_input.send_keys(Keys.TAB)

                # Locate the Download Button
                download_btn = wait.until(EC.element_to_be_clickable((By.ID, "dlbtn")))
                download_btn.click()

                
                time.sleep(0.4) # Wait for download to likely finish
                self._wait_and_process_zip(current_date.strftime("%Y-%m-%d.xls"), final_filename)
                time.sleep(0.3) # Politeness

                print(f"Success! Check your folder: {self.download_folder}")
                current_date += timedelta(days=1)

            except Exception as e:
                print(f"An error occurred: {e}")
                current_date = end_date


    # Update signature to accept the name
    def _wait_and_process_zip(self, target_date_str, final_name=None):
        print("Waiting for download to complete...")
        
        # 1. Wait loop: Search for the newest .zip file
        timeout = 10
        start_time = time.time()
        downloaded_zip = None

        while time.time() - start_time < timeout:
            # Find all zip files in the folder
            zips = glob.glob(os.path.join(self.download_folder, "*.zip"))
            if zips:
                # Pick the most recently created one
                newest_zip = max(zips, key=os.path.getctime)
                # Ensure it's not a temporary file (.crdownload)
                if not newest_zip.endswith('.crdownload'):
                    downloaded_zip = newest_zip
                    break
            time.sleep(1)

        if not downloaded_zip:
            print("Error: Download timed out or failed.")
            return

        # 2. Extract and Rename
        print(f"Extracting {downloaded_zip}...")
        try:
            with zipfile.ZipFile(downloaded_zip, 'r') as zip_ref:
                # Find the .xls file inside the zip
                file_names = zip_ref.namelist()
                xls_files = [f for f in file_names if f.endswith('.xls') or f.endswith('.xlsx')]
                
                if xls_files:
                    original_filename = xls_files[0]
                    zip_ref.extract(original_filename, self.download_folder)
                    
                    # Logic to determine the new name
                    if final_name:
                        new_filename = os.path.join(self.download_folder, final_name)
                    else:
                        new_filename = os.path.join(self.download_folder, f"RTE_{target_date_str}.xls")
                    
                    extracted_path = os.path.join(self.download_folder, original_filename)
                    
                    # Overwrite protection: delete if exists
                    if os.path.exists(new_filename):
                        os.remove(new_filename)
                        
                    os.rename(extracted_path, new_filename)
                    print(f"File saved as: {new_filename}")
                else:
                    print("Warning: No Excel file found in the zip.")

            # 3. Delete the original zip file
            os.remove(downloaded_zip)

        except zipfile.BadZipFile:
            print("Error: The downloaded file was not a valid zip.")



def parse_arguments():
    parser = argparse.ArgumentParser(description="Download RTE data for a specific date.")
    parser.add_argument("--date", type=str, required=True, help="(Starting) Date to download in DD/MM/YYYY format, from 01/01/2012")
    parser.add_argument("--enddate", type=str, help="Optional end date to download in DD/MM/YYYY format")
    parser.add_argument("--output", type=str, default=None, help="Optional: Rename the downloaded file (e.g., data.xls)")
    parser.add_argument("--folder", type=str, default=os.path.join(os.getcwd(), "RTE_daily_data"), help="Download folder path (default: current execution folder)")

    return parser.parse_args()



if __name__ == "__main__":
    args = parse_arguments()
    rte = RTEAPI(args.folder)

    try:
        rte.open_page()
    except Exception as e:
        print(f"Failed to open page: {e}")
        exit(1)

    try:
        print(f"Starting download for date: {args.date}")
        rte.download_data(args.date, args.enddate, final_filename=args.output)
    finally:
        rte.close_page()