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
import traceback

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
LOG_FILE = os.path.join(GDRIVE_BASE_PATH, "scraping_log.txt")
BASE_TARGET_URL = "https://wearequantico.atlassian.net/servicedesk/customer/user/requests?filter=IGE%20IGA&page=1&reporter=all&sortBy=updatedDate&sortOrder=DESC"

# =============================================================================
# FUNCTIONS
# =============================================================================
def parse_relative_date(date_str):
    """Convert Atlassian relative dates to date string format."""
    if not date_str or pd.isna(date_str):
        return ""
    
    date_str = str(date_str).strip().lower()
    today = datetime.now()
    
    # Handle relative dates and convert to actual date
    if date_str == 'today' or date_str == 'oggi':
        return today.strftime('%d/%m/%Y')
    elif date_str == 'yesterday' or date_str == 'ieri':
        return (today - timedelta(days=1)).strftime('%d/%m/%Y')
    elif date_str.startswith('yesterday at') or date_str.startswith('ieri'):
        return (today - timedelta(days=1)).strftime('%d/%m/%Y')
    elif date_str.startswith('today at') or date_str.startswith('oggi'):
        return today.strftime('%d/%m/%Y')
    elif 'ago' in date_str or 'fa' in date_str:
        # Handle "X minutes/hours/days ago" or "X minuti/ore/giorni fa"
        parts = date_str.split()
        if len(parts) >= 2:
            try:
                value = int(parts[0])
                unit = parts[1]
                if 'minute' in unit or 'minuto' in unit or 'minuti' in unit:
                    return today.strftime('%d/%m/%Y')
                elif 'hour' in unit or 'ora' in unit or 'ore' in unit:
                    return today.strftime('%d/%m/%Y')
                elif 'day' in unit or 'giorno' in unit or 'giorni' in unit:
                    return (today - timedelta(days=value)).strftime('%d/%m/%Y')
                elif 'week' in unit or 'settimana' in unit or 'settimane' in unit:
                    return (today - timedelta(weeks=value)).strftime('%d/%m/%Y')
                elif 'month' in unit or 'mese' in unit or 'mesi' in unit:
                    return (today - timedelta(days=value*30)).strftime('%d/%m/%Y')  # Approximate
            except ValueError:
                pass
    
    # Try to parse standard date formats and return in DD/MM/YYYY format
    date_formats = [
        '%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y',
        '%d/%m/%Y %H:%M', '%Y-%m-%d %H:%M', '%m/%d/%Y %H:%M',
        '%d %b %Y', '%d %B %Y', '%b %d, %Y', '%B %d, %Y',
        '%d/%m/%y', '%d/%b/%Y'  # Add short year format
    ]
    
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            return parsed_date.strftime('%d/%m/%Y')
        except ValueError:
            continue
    
    # If all else fails, return original string (might already be a date)
    return date_str

def get_historical_filename():
    """Generate historical filename with timestamp."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return os.path.join(GDRIVE_BASE_PATH, f"tickets_{timestamp}.csv")

def get_existing_ticket_count():
    """Get the number of tickets in the existing master file."""
    try:
        if os.path.exists(MASTER_FILE):
            df = pd.read_csv(MASTER_FILE)
            return len(df)
        return 0
    except Exception:
        return 0

def save_with_fault_tolerance(df):
    """Save tickets with fault tolerance logic."""
    existing_count = get_existing_ticket_count()
    new_count = len(df)
    
    print(f"Existing tickets: {existing_count}, New tickets: {new_count}")
    
    # Always save historical copy
    historical_file = get_historical_filename()
    df.to_csv(historical_file, index=False, encoding='utf-8-sig')
    print(f"Historical backup saved to: {historical_file}")
    
    # Check if new data has fewer tickets (potential error)
    if new_count < existing_count:
        # Save as "bad" file and don't overwrite master
        bad_file = historical_file.replace('.csv', '_bud.csv')
        df.to_csv(bad_file, index=False, encoding='utf-8-sig')
        print(f"WARNING: Fewer tickets found! Bad data saved to: {bad_file}")
        print(f"Master file NOT updated to prevent data loss.")
        return False
    else:
        # Save as master file
        atomic_save_csv(df, MASTER_FILE)
        print(f"Master file updated successfully with {new_count} tickets.")
        return True

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

def write_log(message, success=True):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    status = "SUCCESS" if success else "ERROR"
    log_entry = f"[{timestamp}] {status}: {message}\n"
    
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_entry)
    except Exception as e:
        print(f"Failed to write to log file: {e}")

def atomic_save_csv(df, filepath):
    """Save CSV file atomically - write to temp file first, then rename."""
    temp_file = filepath + '.tmp'
    backup_file = filepath + '.backup'
    
    try:
        # Create backup of existing file if it exists
        if os.path.exists(filepath):
            if os.path.exists(backup_file):
                os.remove(backup_file)
            os.rename(filepath, backup_file)
        
        # Write to temporary file
        df.to_csv(temp_file, index=False, encoding='utf-8-sig')
        
        # Atomic rename
        os.rename(temp_file, filepath)
        
        # Remove backup if successful
        if os.path.exists(backup_file):
            os.remove(backup_file)
            
        return True
        
    except Exception as e:
        # Restore from backup if something went wrong
        if os.path.exists(backup_file):
            if os.path.exists(filepath):
                os.remove(filepath)
            os.rename(backup_file, filepath)
        
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
            
        raise e

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

def scrape_all_tickets(driver):
    """Scrape all tickets from Atlassian service desk using infinite scroll."""
    driver.get(BASE_TARGET_URL)
    time.sleep(4) # Wait for initial page loading
    
    # Scroll down to load all tickets FIRST, then extract data
    print("Loading all tickets by scrolling...")
    last_height = driver.execute_script("return document.body.scrollHeight")
    scroll_attempts = 0
    max_scroll_attempts = 50  # Prevent infinite loops
    
    while scroll_attempts < max_scroll_attempts:
        # Scroll to bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        # Wait for new content to load
        time.sleep(2)  # Reduced wait time
        
        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
        scroll_attempts += 1
        print(f"Scroll attempt {scroll_attempts}, height: {new_height}")
    
    print("Finished scrolling. Extracting ticket data...")
    # Extract all tickets AFTER fully loading the page
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
            'created': parse_relative_date(cols[6].get_text(strip=True)) if len(cols) > 6 else '',
            'updated': parse_relative_date(cols[7].get_text(strip=True)) if len(cols) > 7 else '',
            'priority': cols[8].get_text(strip=True) if len(cols) > 8 else '',
            'url': link.get('href', '') if link else ''
        })
    return tickets

# =============================================================================
# MERGE AND SAVE LOGIC
# =============================================================================
def main():
    """Main function to orchestrate the complete ticket scraping process."""
    start_time = datetime.now()
    driver = None
    all_tickets = []
    success = False
    error_message = ""
    
    try:
        write_log("Starting ticket scraping session")
        driver = setup_driver()
        
        if not fast_login(driver):
            error_message = "Failed to login to Atlassian"
            write_log(error_message, success=False)
            return

        # Scrape all tickets at once using infinite scroll
        print("Scraping all tickets (this may take a while)...")
        all_tickets = scrape_all_tickets(driver)
        print(f"Found {len(all_tickets)} total tickets")

        if all_tickets:
            final_df = pd.DataFrame(all_tickets)
            
            # Remove any duplicates (based on KEY)
            final_df.drop_duplicates(subset=['key'], keep='first', inplace=True)
            
            # Sort to put highest IGA ticket at top
            final_df = get_highest_iga_ticket(final_df)

            # Save with fault tolerance
            save_success = save_with_fault_tolerance(final_df)
            
            if save_success:
                end_time = datetime.now()
                duration = end_time - start_time
                
                success_message = f"Scraping completed successfully. {len(final_df)} tickets saved in {duration.total_seconds():.1f}s"
                write_log(success_message, success=True)
                print(success_message)
                success = True
            else:
                error_message = "Fault tolerance prevented master file update due to fewer tickets"
                write_log(error_message, success=False)
                print(error_message)
        else:
            error_message = "No tickets found during scraping"
            write_log(error_message, success=False)
            print(error_message)

    except Exception as e:
        error_message = f"Unexpected error during scraping: {str(e)}"
        write_log(f"{error_message}\n{traceback.format_exc()}", success=False)
        print(f"ERROR: {error_message}")
        
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
        
        # Log final status if not already logged
        if not success and error_message:
            end_time = datetime.now()
            duration = end_time - start_time
            write_log(f"Session failed after {duration.total_seconds():.1f}s: {error_message}", success=False)

if __name__ == "__main__":
    main()