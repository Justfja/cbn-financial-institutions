import logging
from datetime import datetime
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
from urllib.parse import urljoin
import random
import os


## Logging 
def setup_logging():
    # Create logs directory if it doesn't exist
    os.makedirs('data/logs', exist_ok=True)
    
    # Get current date for log filename
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'data/logs/cbn_scraper_{today}.log'),
            logging.StreamHandler()  # Also output to console
        ]
    )
    
    return logging.getLogger()



## Set up logger
logger = setup_logging()

# Base URL
base_url = "https://www.cbn.gov.ng/supervision/"

# List of institution categories with their URLs
categories = [
    {"name": "Commercial Banks", "url": "Inst-DM.html"},
    {"name": "Development Finance Institutions", "url": "Inst-DFI.html"},
    {"name": "Discount Houses", "url": "Inst-DH.html"},
    {"name": "Finance Companies", "url": "Inst-FC.html"},
    {"name": "Holding Companies", "url": "Inst-HC.html"},
    {"name": "Merchant Banks", "url": "Inst-MB.html"},
    {"name": "Micro-finance Banks", "url": "Inst-MF.html"},
    {"name": "Non-Interest Banks", "url": "Inst-NI.html"},
    {"name": "Primary Mortgage Banks", "url": "Inst-PMI.html"},
    {"name": "Payment Service Banks", "url": "Inst-PSB.html"},
    {"name": "Mobile Money Operators", "url": "Inst-MMO.html"}
]


def setup_driver():
    """
    Set up and return a configured Chrome WebDriver
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-images")                      # New
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")  # New
    chrome_options.add_argument("--proxy-server='direct://'")
    chrome_options.add_argument("--proxy-bypass-list=*")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--ignore-certificate-errors")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(30)
    return driver


def wait_for_table_to_load(driver):
    """
    Wait for the table with data to load
    """
    try:
        # Wait for table structure
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".k-grid-table"))
        )
        
        # Wait for at least one row in the table
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".k-grid-table tbody tr"))
        )
        
        # Extra wait for JS rendering
        time.sleep(2)
        return True
    except TimeoutException:
        logger.error("Timed out waiting for table to load")
        return False
    


def get_max_page_number(driver):
    """
    Get the maximum page number available
    """
    try:
        # Try to find the last page button which often has the max page number
        last_page_button = driver.find_element(By.CSS_SELECTOR, ".k-pager-last")
        page_number = last_page_button.get_attribute("data-page")
        if page_number and page_number.isdigit():
            return int(page_number)
        
        # Alternative: Find all page number buttons and get the highest
        page_buttons = driver.find_elements(By.CSS_SELECTOR, ".k-pager-numbers .k-button")
        max_page = 1
        for button in page_buttons:
            data_page = button.get_attribute("data-page")
            if data_page and data_page.isdigit() and int(data_page) > max_page:
                max_page = int(data_page)
        
        return max_page if max_page > 1 else 1
    except (NoSuchElementException, Exception) as e:
        logger.error(f"Could not determine max page number: {str(e)}")
        # Assume at least one page
        return 1  


def extract_links_from_table(driver):
    """
    Extract all institution links from the current table view
    """
    links = []
    try:
        # Wait for table content to be visible
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".k-grid-table tbody tr"))
        )
        
        # Allow extra time for all rows to render
        time.sleep(2)
        
        # Try different selectors to find links
        selectors = [
            ".k-grid-table tbody tr td a[href*='fi.html']",
            "table tbody tr td a[href*='fi.html']",
            "//a[contains(@href, 'fi.html')]"
        ]
        
        link_elements = []
        for selector in selectors:
            if selector.startswith("//"):
                link_elements = driver.find_elements(By.XPATH, selector)
            else:
                link_elements = driver.find_elements(By.CSS_SELECTOR, selector)
            
            if link_elements:
                break
                
        if not link_elements:
            logger.error("Could not find any institution links in the table")
            return []
            
        for link in link_elements:
            institution_name = link.text.strip()
            if institution_name:  # Only include non-empty names
                institution_url = urljoin(base_url, link.get_attribute('href'))
                links.append({
                    'name': institution_name,
                    'url': institution_url
                })
                
        logger.info(f"Found {len(links)} institution links in current page")
        
    except Exception as e:
        logger.error(f"Error extracting links from table: {str(e)}")
        
    return links


def handle_pagination(driver):
    """
    Navigate through all pagination pages and extract links from each page
    """
    all_links = []
    
    # Get initial links from first page
    current_page_links = extract_links_from_table(driver)
    all_links.extend(current_page_links)
    logger.info(f"Extracted {len(current_page_links)} links from page 1")
    
    # Determine how many pages we need to navigate through
    max_page = get_max_page_number(driver)
    logger.info(f"Detected {max_page} total pages of institutions")
    
    # If only one page, we're already done
    if max_page <= 1:
        return all_links
    
    # Navigate through additional pages (starting from page 2)
    for page_num in range(2, max_page + 1):
        try:
            # Find the specific page button by its data-page attribute
            page_selector = f".k-pager-numbers .k-button[data-page='{page_num}']"
            try:
                page_button = driver.find_element(By.CSS_SELECTOR, page_selector)
                driver.execute_script("arguments[0].click();", page_button)
            except NoSuchElementException:
                # If specific page button not found, try next button
                next_button = driver.find_element(By.CSS_SELECTOR, ".k-pager-nav.k-pager-next")
                driver.execute_script("arguments[0].click();", next_button)
            
            logger.info(f"Navigated to page {page_num}")
            
            # Wait for table to update after pagination
            wait_for_table_to_load(driver)
            
            # Extract links from current page
            page_links = extract_links_from_table(driver)
            all_links.extend(page_links)
            logger.info(f"Extracted {len(page_links)} links from page {page_num}")
            
            # Small delay to avoid rate limiting
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"Error navigating to page {page_num}: {str(e)}")
            break
    
    return all_links


def try_select_all_via_kendo_dropdown(driver):
    """Try to select 'All' using Kendo UI dropdown interactions"""
    try:
        # Find the Kendo dropdown (not the hidden select)
        dropdown = driver.find_element(By.CSS_SELECTOR, ".k-pager-sizes .k-dropdownlist")
        
        # Click to open the dropdown
        driver.execute_script("arguments[0].click();", dropdown)
        time.sleep(2)
        
        # Wait for dropdown list to appear
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".k-list-container.k-popup"))
        )
        
        # Try to find and click the "All" option in the popup list
        try:
            # Various selectors that might match the "All" option
            all_option_selectors = [
                ".k-list-container.k-popup .k-list-item:first-child",
                ".k-list-container.k-popup li:first-child",
                "//div[contains(@class, 'k-popup')]//li[contains(text(), 'All')]",
                "//div[contains(@class, 'k-popup')]//li[1]"
            ]
            
            for selector in all_option_selectors:
                try:
                    if selector.startswith("//"):
                        all_option = driver.find_element(By.XPATH, selector)
                    else:
                        all_option = driver.find_element(By.CSS_SELECTOR, selector)
                    
                    driver.execute_script("arguments[0].click();", all_option)
                    print("Selected 'All' option from Kendo dropdown")
                    time.sleep(3)
                    return True
                except NoSuchElementException:
                    continue
        except Exception as e:
            logger.error(f"Failed to select option from Kendo dropdown: {str(e)}")
            
        # If we made it here, we couldn't select "All" but at least we clicked the dropdown
        return False
            
    except Exception as e:
        logger.error(f"Could not interact with Kendo dropdown: {str(e)}")
        return False


def extract_institution_links(driver, category_url):
    """
    Extract all institution links using pagination
    """
    full_url = urljoin(base_url, category_url)
    logger.info(f"Fetching category URL: {full_url}")
    
    try:
        # Navigate to category page
        driver.get(full_url)
        
        # Wait for initial table load
        if not wait_for_table_to_load(driver):
            logger.error(f"Failed to load table at {full_url}")
            return []
            
        # First try: attempt to select "All" via Kendo dropdown
        try_select_all_via_kendo_dropdown(driver)
        
        # Wait for potential table reload
        time.sleep(5)
        
        # If we successfully selected "All", we should have all links in one page
        initial_links = extract_links_from_table(driver)
        if len(initial_links) > 15:  # If we got a large number of links, "All" likely worked
            logger.info(f"Found {len(initial_links)} links after selecting 'All'")
            return initial_links
            
        # Otherwise, use pagination approach
        logger.info("Using pagination approach to extract all links")
        return handle_pagination(driver)
            
    except Exception as e:
        logger.error(f"Error extracting links from {category_url}: {str(e)}")
        return []


def extract_institution_details(driver, institution_url, institution_name, category_name):
    """Extract details from an individual institution page"""
    logger.info(f"Fetching institution URL: {institution_url}")
    try:
        driver.get(institution_url)
    except Exception as e:
        logger.error(f"Error loading URL {institution_url}: {str(e)}")
        return None
    
    # Wait for content to load
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".postcontent"))
        )
    except TimeoutException:
        try:
            # Alternative: wait for any table
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
        except TimeoutException:
            logger.error(f"No content loaded at {institution_url}") 
            return None
    
    # Allow time for page to fully render
    time.sleep(3)
    
    try:
        # Try to find tables within postcontent
        post_content = driver.find_element(By.CSS_SELECTOR, ".postcontent")
        tables = post_content.find_elements(By.TAG_NAME, "table")
        
        # If not found, look for tables anywhere
        if len(tables) < 2:
            tables = driver.find_elements(By.TAG_NAME, "table")
        
        if len(tables) < 2:
            logger.error(f"Could not find at least 2 tables for {institution_name}")
            return None
        
        # Extract labels from the first table
        labels = [td.text.strip() for td in tables[0].find_elements(By.TAG_NAME, "td")]
        
        # Extract values from the second table
        values = [td.text.strip() for td in tables[1].find_elements(By.TAG_NAME, "td")]
        
        # Create a dictionary of institution details
        details = {'Institution Name': institution_name, 'Category': category_name}
        
        # Map labels to values
        for i, label in enumerate(labels):
            if i < len(values):
                details[label] = values[i]
            else:
                details[label] = ''
        
        logger.info(f"Successfully extracted details for {institution_name}")
        return details
    
    except Exception as e:
        logger.error(f"Error extracting details for {institution_name}: {str(e)}")
        return None


def scrape_category(driver, category):
    """
    Scrape all institutions within a category
    """
    logger.info(f"\nScraping category: {category['name']}")
    institution_links = extract_institution_links(driver, category['url'])
    
    category_institutions = []
    if not institution_links:
        logger.error(f"No links found for category: {category['name']}")
        return category_institutions
    
    for i, institution in enumerate(institution_links):
        try:
            logger.info(f"  Scraping institution {i+1}/{len(institution_links)}: {institution['name']}")
            details = extract_institution_details(
                driver, 
                institution['url'], 
                institution['name'],
                category['name']
            )
            
            if details:
                category_institutions.append(details)
                logger.info(f"  Successfully added {institution['name']} to dataset")
            else:
                logger.error(f"  Failed to extract details for {institution['name']}")
            
            # Adding delay to avoid overloading the server
            time.sleep(random.uniform(2, 4))
        except Exception as e:
            logger.error(f"Error processing institution {institution['name']}: {str(e)}")
    
    logger.info(f"Collected {len(category_institutions)} institutions for {category['name']}")
    return category_institutions


def main():
    """
    The Main function to orchestrate the end-to-end scraping process.
    """

    # Create output directory structure for CSVs
    os.makedirs('data/cbn_data', exist_ok=True)
    
    driver = setup_driver()
    all_institutions = []
    
    try:
        # Process each category
        for i, category in enumerate(categories):
            # Uncomment to limit categories for testing
            # if i >= 2:
            #     break
                
            # Try up to 3 times for each category
            max_retries = 2
            success = False
            
            for attempt in range(max_retries + 1):
                try:
                    category_institutions = scrape_category(driver, category)
                    if category_institutions:
                        all_institutions.extend(category_institutions)
                        
                        # Save category data
                        category_df = pd.DataFrame(category_institutions)
                        clean_name = category['name'].lower().replace(' ', '_').replace('(', '').replace(')', '').replace("'", "")
                        filename = f"data/cbn_data/cbn_{clean_name}.csv"
                        category_df.to_csv(filename, index=False)
                        logger.info(f"Saved {len(category_df)} records to {filename}")
                        
                        success = True
                        break
                except Exception as e:
                    if attempt < max_retries:
                        logger.error(f"Error scraping category {category['name']}, attempt {attempt+1}/{max_retries+1}: {str(e)}")
                        # Wait before retry
                        time.sleep(10)  
                    else:
                        logger.error(f"Failed to scrape category {category['name']} after {max_retries+1} attempts")
    
    except Exception as e:
        logger.error(f"Error during main scraping process: {str(e)}")
    
    finally:
        # Save all data regardless of errors
        if all_institutions:
            # Get today's date for filenames
            today = datetime.now().strftime('%Y-%m-%d')

            all_df = pd.DataFrame(all_institutions)
            # all_df.to_csv('cbn_data/cbn_all_financial_institutions.csv', index=False)
            all_df.to_csv(f'data/cbn_data/cbn_all_financial_institutions_{today}.csv', index=False)
            
            # Create a copy as "latest" for easy reference
            all_df.to_csv('data/cbn_data/cbn_all_financial_institutions_latest.csv', index=False)

            logger.info(f"\nScraped {len(all_df)} total institutions and saved to 'data/cbn_data/cbn_all_financial_institutions.csv'")
        
        # Clean up
        driver.quit()



if __name__ == "__main__":
    main()