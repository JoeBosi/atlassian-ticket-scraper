import os
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================
GDRIVE_BASE_PATH = os.getenv('GDRIVE_BASE_PATH')
ATLASSIAN_SITE = os.getenv('ATLASSIAN_SITE')
EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')

MASTER_FILE = os.path.join(GDRIVE_BASE_PATH, "tickets.csv")
BASE_TARGET_URL = f"https://{ATLASSIAN_SITE}.atlassian.net/servicedesk/customer/user/requests?reporter=all&sortBy=createdDate&sortOrder=DESC"

# =============================================================================
# FUNCTIONS
# =============================================================================
def get_last_scraped_key():
    """Get the key of the last scraped ticket from master file."""
    if not os.path.exists(MASTER_FILE): return None
    try:
        df = pd.read_csv(MASTER_FILE)
        return str(df.iloc[0]['key']) if not df.empty else None
    except: return None

def setup_driver():
    """Setup and configure Chrome WebDriver."""
    opts = Options()
    # opts.add_argument("--headless") 
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def fast_login(driver):
    """Perform fast login to Atlassian service desk."""
    try:
        driver.get(f"https://{ATLASSIAN_SITE}.atlassian.net/servicedesk/customer/portal/49/user/login")
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.ID, "user-email"))).send_keys(EMAIL)
        driver.find_element(By.XPATH, "//button[@type='submit']").click()
        time.sleep(2)
        wait.until(EC.presence_of_element_located((By.XPATH, "//input[@type='password']"))).send_keys(PASSWORD)
        driver.find_element(By.XPATH, "//button[@type='submit']").click()
        time.sleep(5)
        return True
    except: return False

def scrape_page(driver, page_num):
    """Scrape a single page of tickets from Atlassian service desk."""
    driver.get(f"{BASE_TARGET_URL}&page={page_num}")
    time.sleep(4) # Wait for dynamic table loading
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    tickets = []
    # Find all table rows
    rows = soup.find_all('tr')
    
    for row in rows:
        # CRITICAL CHECK: If row has 'th' tag, it's a header. Skip it.
        if row.find('th'):
            continue
            
        cols = row.find_all('td')
        # If row doesn't have enough columns, it's not a valid ticket
        if len(cols) < 5:
            continue
        
        link = row.find('a')
        key = link.get_text(strip=True) if link else cols[0].get_text(strip=True)
        
        # Clean the Key (e.g., remove spaces or strange characters)
        key = key.replace('\n', '').strip()
        
        tickets.append({
            'key': key,
            'reference': cols[1].get_text(strip=True),
            'summary': cols[2].get_text(strip=True),
            'status': cols[3].get_text(strip=True),
            'type': cols[0].get_text(strip=True),
            'service_desk': cols[4].get_text(strip=True),
            'requester': cols[5].get_text(strip=True) if len(cols) > 5 else '',
            'created': cols[6].get_text(strip=True) if len(cols) > 6 else '',
            'updated': cols[7].get_text(strip=True) if len(cols) > 7 else '',
            'priority': cols[8].get_text(strip=True) if len(cols) > 8 else '',
            'url': link.get('href', '') if link else ''
        })
    return tickets

# =============================================================================
# MERGE AND SAVE LOGIC
# =============================================================================
def main():
    """Main function to orchestrate the ticket scraping process."""
    last_key = get_last_scraped_key()
    driver = setup_driver()
    new_tickets = []
    
    try:
        if not fast_login(driver): return

        page = 1
        found_old = False
        while page <= 50: # Page limit
            print(f"Analyzing page {page}...")
            page_data = scrape_page(driver, page)
            
            if not page_data: break
            
            for t in page_data:
                if last_key and t['key'] == last_key:
                    print(f"Found anchor point: {last_key}. Stopping.")
                    found_old = True
                    break
                new_tickets.append(t)
            
            if found_old: break
            page += 1

        if new_tickets:
            new_df = pd.DataFrame(new_tickets)
            if os.path.exists(MASTER_FILE):
                old_df = pd.read_csv(MASTER_FILE)
                # Put new tickets ABOVE old ones
                final_df = pd.concat([new_df, old_df], ignore_index=True)
            else:
                final_df = new_df
            
            # Remove duplicates for safety (based on KEY)
            final_df.drop_duplicates(subset=['key'], keep='first', inplace=True)
            
            # FINAL SORTING: Ensure most recent are at top
            # If key is like "ABC-123", descending alphabetical/numeric sorting works
            final_df = final_df.sort_values(by='key', ascending=False)

            final_df.to_csv(MASTER_FILE, index=False, encoding='utf-8-sig')
            print(f"Save completed. {len(new_tickets)} new tickets added.")
        else:
            print("All up to date. No new tickets.")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()