import os
import time
import csv
import json
import logging
import argparse
import concurrent.futures
from queue import Queue
from functools import wraps
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, NoSuchElementException

# --- Global Settings ---
OUTPUT_FOLDER = "parsed_data"
CONFIG_FILE = "config/segments_beauty.json"
DEFAULT_SEGMENT = "beauty_micro"
DEFAULT_FORMAT = "csv"

# --- Logger Configuration ---
def setup_logger():
    """Sets up the logger for the application."""
    log_filename = f'parsing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logger()

# --- Configuration Loading ---
def load_config(config_path):
    """Loads the segmentation and scoring configuration from a JSON file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found at: {config_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from the configuration file: {config_path}")
        raise

# --- Lead Scoring and Segmentation ---
def calculate_lead_score(company_data, segment_config):
    """Calculates the lead score for a company based on the provided segment configuration."""
    score = 0
    scoring_rules = segment_config.get("scoring", {})

    # Rule: Belongs to a target beauty category
    if any(keyword.lower() in company_data.get("category", "").lower() for keyword in segment_config.get("include_keywords", [])):
        score += scoring_rules.get("base_beauty", 0)

    # Rule: Short name without stop words
    if not any(stop_word.lower() in company_data.get("name", "").lower() for stop_word in segment_config.get("exclude_keywords", [])):
        score += scoring_rules.get("short_name", 0)

    # Rule: Single address (simplified check)
    # This is a placeholder as the current scraping logic doesn't provide the number of branches.
    # We'll assume a single address for now.
    score += scoring_rules.get("single_address", 0)

    # Rule: Has at least one phone number
    if company_data.get("has_phone"):
        score += scoring_rules.get("has_phone", 0)

    # Rule: Mentions of online booking systems
    if any(keyword.lower() in company_data.get("description", "").lower() for keyword in segment_config.get("online_booking_keywords", [])):
        score += scoring_rules.get("has_online_booking", 0)

    return max(0, min(100, score))

def assign_segment(lead_score, thresholds):
    """Assigns a segment to a company based on its lead score."""
    if lead_score >= thresholds.get("beauty_micro", 80):
        return "beauty_micro"
    elif lead_score >= thresholds.get("beauty_mid", 50):
        return "beauty_mid"
    else:
        return "other"

# --- Selenium WebDriver and Utilities ---
def retry(max_attempts=3, delay=0.5, backoff=2):
    """Decorator for retrying a function in case of exceptions."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    logger.warning(f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {e}")
                    if attempt >= max_attempts:
                        logger.error(f"All {max_attempts} attempts failed for {func.__name__}.")
                        raise
                    time.sleep(delay * (backoff ** (attempt - 1)))
            return None
        return wrapper
    return decorator

def setup_driver():
    """Initializes and returns a Chrome WebDriver."""
    logger.info("Initializing WebDriver...")
    chrome_options = Options()
    # chrome_options.add_argument("--headless=new") # Disabled for debugging
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        return driver
    except Exception as e:
        logger.error(f"Error creating WebDriver: {e}")
        raise

# --- Data Extraction from 2GIS ---
@retry
def extract_company_details(driver, company_url):
    """Extracts detailed information from a company's 2GIS page."""
    driver.get(company_url)
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='_z9f3z9']")))

    details = {
        "phones": [],
        "site": None,
        "socials": [],
        "schedule": None,
        "description": None,
    }

    # Extract phones
    try:
        phone_elements = driver.find_elements(By.CSS_SELECTOR, "div[class*='_b0ke8'] a[href^='tel:']")
        details["phones"] = list(set([elem.get_attribute("href").replace("tel:", "") for elem in phone_elements]))
    except Exception:
        pass

    # Extract site and socials
    try:
        link_elements = driver.find_elements(By.CSS_SELECTOR, "div[class*='_13ayb0w'] a")
        for link in link_elements:
            href = link.get_attribute("href")
            if not href:
                continue
            if "vk.com" in href or "instagram.com" in href or "facebook.com" in href:
                if href not in details["socials"]:
                    details["socials"].append(href)
            elif "2gis.ru" not in href and "tel:" not in href and "mailto:" not in href:
                details["site"] = href
    except Exception:
        pass

    # Extract schedule
    try:
        details["schedule"] = driver.find_element(By.CSS_SELECTOR, "div[class*='_p698x2']").text
    except Exception:
        pass

    # Extract description
    try:
        details["description"] = driver.find_element(By.CSS_SELECTOR, "div[class*='_h29xsh']").text
    except Exception:
        pass
        
    return details


# --- Main Application Logic ---
def main():
    """Main function to run the 2GIS parser."""
    parser = argparse.ArgumentParser(description="2GIS Parser for Beauty Niche Leads")
    parser.add_argument("--city", type=str, required=True, help="City name, e.g., 'Екатеринбург'")
    parser.add_argument("--segment", type=str, default=DEFAULT_SEGMENT, help=f"Segment to target (default: {DEFAULT_SEGMENT})")
    parser.add_argument("--limit", type=int, default=500, help="Maximum number of companies to parse")
    parser.add_argument("--output", type=str, required=True, help="Path to the output file")
    parser.add_argument("--format", type=str, default=DEFAULT_FORMAT, choices=["csv", "json"], help="Output format")

    args = parser.parse_args()

    logger.info(f"Starting 2GIS parser with the following settings:")
    logger.info(f"  City: {args.city}")
    logger.info(f"  Segment: {args.segment}")
    logger.info(f"  Limit: {args.limit}")
    logger.info(f"  Output File: {args.output}")
    logger.info(f"  Format: {args.format}")

    config = load_config(CONFIG_FILE)
    segment_config = config["segments"].get(args.segment)
    if not segment_config:
        logger.error(f"Segment '{args.segment}' not found in the configuration file.")
        return

    driver = setup_driver()
    all_results = []
    
    try:
        for keyword in segment_config["include_keywords"]:
            if len(all_results) >= args.limit:
                break

            search_query = f"{keyword} {args.city}"
            logger.info(f"Searching for: {search_query}")
            
            driver.get("https://2gis.ru")
            try:
                search_input = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[placeholder='Поиск в 2ГИС']"))
                )
                search_input.clear()
                search_input.send_keys(search_query)
                search_input.send_keys(Keys.ENTER)
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='_awhq0s']"))
                )
                time.sleep(2) # Allow search results to load
            except (TimeoutException, NoSuchElementException) as e:
                logger.error(f"Failed to perform search for '{search_query}': {type(e).__name__} - {e}")
                continue

            last_height = driver.execute_script("return document.body.scrollHeight")
            while len(all_results) < args.limit:
                company_elements = driver.find_elements(By.CSS_SELECTOR, "div[class*='_awhq0s']")
                
                for elem in company_elements:
                    if len(all_results) >= args.limit:
                        break
                    
                    try:
                        name_elem = elem.find_element(By.CSS_SELECTOR, "a[class*='_1rehek']")
                        name = name_elem.text
                        link = name_elem.get_attribute("href")
                        
                        if not name or not link:
                            continue

                        address = elem.find_element(By.CSS_SELECTOR, "div[class*='_1p8ik38']").text
                        
                        try:
                            category = elem.find_element(By.CSS_SELECTOR, "span[class*='_m0an35']").text
                        except NoSuchElementException:
                            category = "N/A" # Category might not be present

                        if link in [res.get("link") for res in all_results]:
                            continue

                        logger.info(f"Processing: {name}")

                        company_data = {
                            "name": name,
                            "link": link,
                            "address": address,
                            "category": category,
                            "city": args.city,
                            "source": "2gis",
                        }
                        
                        all_results.append(company_data)

                    except StaleElementReferenceException:
                        continue
                    except Exception as e:
                        logger.warning(f"Could not process an element: {type(e).__name__} - {e}")

                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
        
        # --- Detail Extraction Loop ---
        logger.info(f"Collected {len(all_results)} companies. Now extracting details...")
        for company_data in all_results:
            if company_data.get("phones"): # Skip if already processed
                continue
            try:
                details = extract_company_details(driver, company_data["link"])
                company_data.update(details)
                
                company_data["has_phone"] = bool(company_data["phones"])
                
                lead_score = calculate_lead_score(company_data, segment_config)
                segment = assign_segment(lead_score, config["thresholds"])
                
                company_data["lead_score"] = lead_score
                company_data["segment"] = segment
            except Exception as e:
                logger.error(f"Failed to extract details for {company_data['name']}: {type(e).__name__} - {e}")


    finally:
        driver.quit()

    if all_results:
        if not os.path.exists(OUTPUT_FOLDER):
            os.makedirs(OUTPUT_FOLDER)
        
        # Flatten data for CSV
        final_results = []
        for res in all_results:
            final_results.append({
                "name": res.get("name"),
                "city": res.get("city"),
                "address": res.get("address"),
                "phones": ",".join(res.get("phones", [])),
                "site": res.get("site"),
                "socials": ",".join(res.get("socials", [])),
                "schedule": res.get("schedule"),
                "category": res.get("category"),
                "description": res.get("description"),
                "link": res.get("link"),
                "segment": res.get("segment"),
                "lead_score": res.get("lead_score"),
                "has_phone": res.get("has_phone"),
                "source": res.get("source"),
            })

        output_path = args.output
        if not output_path.startswith(OUTPUT_FOLDER):
            output_path = os.path.join(OUTPUT_FOLDER, os.path.basename(output_path))
            
        if args.format == "csv":
            fieldnames = final_results[0].keys()
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(final_results)
        elif args.format == "json":
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(final_results, f, ensure_ascii=False, indent=4)
                
        logger.info(f"Successfully parsed and saved {len(final_results)} companies to {output_path}")
    else:
        logger.info("No companies were parsed.")

if __name__ == "__main__":
    main()
