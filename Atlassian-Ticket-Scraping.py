import os
import time
import pandas as pd
from datetime import datetime, timedelta
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
def parse_relative_date(date_str):
    """Convert Atlassian relative dates to datetime objects."""
    if not date_str or pd.isna(date_str):
        return None
    
    date_str = str(date_str).strip().lower()
    today = datetime.now()
    
    # Handle relative dates
    if date_str == 'today':
        return today
    elif date_str == 'yesterday':
        return today - timedelta(days=1)
    elif date_str.startswith('yesterday at'):
        return today - timedelta(days=1)
    elif date_str.startswith('today at'):
        return today
    elif 'ago' in date_str:
        # Handle "X minutes/hours/days ago"
        parts = date_str.split()
        if len(parts) >= 3:
            try:
                value = int(parts[0])
                unit = parts[1]
                if 'minute' in unit:
                    return today - timedelta(minutes=value)
                elif 'hour' in unit:
                    return today - timedelta(hours=value)
                elif 'day' in unit:
                    return today - timedelta(days=value)
                elif 'week' in unit:
                    return today - timedelta(weeks=value)
                elif 'month' in unit:
                    return today - timedelta(days=value*30)  # Approximate
            except ValueError:
                pass
    
    # Try to parse standard date formats
    date_formats = [
        '%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y',
        '%d/%m/%Y %H:%M', '%Y-%m-%d %H:%M', '%m/%d/%Y %H:%M',
        '%d %b %Y', '%d %B %Y', '%b %d, %Y', '%B %d, %Y'
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    # If all else fails, return original string
    return date_str

def get_highest_iga_ticket(df):
    """Find the highest IGA ticket and move it to top."""
    if df.empty:
        return df
    
    # Filter IGA tickets
    iga_tickets = df[df['key'].str.startswith('IGA-', na=False)]
    
    if iga_tickets.empty:
        return df
    
    # Find the highest IGA ticket (by numeric part)
    def extract_number(key):
        try:
            return int(key.split('-')[1])
        except:
            return 0
    
    highest_iga_idx = iga_tickets['key'].apply(extract_number).idxmax()
    highest_iga_row = df.loc[highest_iga_idx]
    
    # Remove it from original position
    df = df.drop(highest_iga_idx)
    
    # Add it to top
    df = pd.concat([pd.DataFrame([highest_iga_row]).reset_index(drop=True), df.reset_index(drop=True)], ignore_index=True)
    
    return df

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
            'created': parse_relative_date(cols[6].get_text(strip=True)) if len(cols) > 6 else None,
            'updated': parse_relative_date(cols[7].get_text(strip=True)) if len(cols) > 7 else None,
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
        anchor_page = None
        
        while page <= 50: # Page limit
            print(f"Analyzing page {page}...")
            page_data = scrape_page(driver, page)
            
            if not page_data: break
            
            for t in page_data:
                if last_key and t['key'] == last_key and anchor_page is None:
                    print(f"Found anchor point: {last_key} on page {page}.")
                    anchor_page = page
                    found_old = True
            
            # Always add tickets from first 2 pages, or until anchor if found after page 2
            if page <= 2 or (anchor_page is None or anchor_page > 2):
                new_tickets.extend(page_data)
            elif found_old and anchor_page <= 2:
                # If anchor found in first 2 pages, only add tickets before anchor
                for t in page_data:
                    if last_key and t['key'] == last_key:
                        break
                    new_tickets.append(t)
                break
            
            if found_old and anchor_page is not None and anchor_page > 2:
                break
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
            final_df = get_highest_iga_ticket(final_df)

            final_df.to_csv(MASTER_FILE, index=False, encoding='utf-8-sig')
            print(f"Save completed. {len(new_tickets)} new tickets added.")
        else:
            print("All up to date. No new tickets.")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()