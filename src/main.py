
import argparse
import csv
import time
import re
import traceback
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# --- Global Configuration & Dictionaries ---

CITY_SLUGS = {
    "санкт-петербург": "spb", "москва": "moscow", "новосибирск": "novosibirsk",
    "екатеринбург": "ekaterinburg", "казань": "kazan", "нижний новгород": "n_novgorod",
    "красноярск": "krasnoyarsk", "челябинск": "chelyabinsk", "самара": "samara",
    "уфа": "ufa", "краснодар": "krasnodar", "омск": "omsk", "пермь": "perm",
    "ростов-на-дону": "rostov", "воронеж": "voronezh", "волгоград": "volgograd",
    "тюмень": "tyumen", "сочи": "sochi", "тбилиси": "tbilisi"
}

# --- New Selector Discovery Functions ---

def discover_selectors(driver):
    """
    Analyzes the DOM to find the most likely selectors for company data.
    """
    print("\n--- Discovering Selectors ---")
    
    selector_strategies = [
        {
            'strategy_name': 'Modern Div-based Wrapper',
            'card': "div._1kf6gff",
            'name': "div._zjunba",
            'address': "span._sfdp8cg", # More specific address selector
            'category': "div._1idnaau",
            'rating': "div._1az2g0c",
            'scroll_container': 'div._1r0xc1d'
        },
        {
            'strategy_name': 'Direct Link-based (a tag)',
            'card': "a._1re_r0w",
            'name': "span._1al0wlf",
            'address': "span._1w9o8ge",
            'category': "span._1w9o8ge",
            'rating': "span._1nqa2pr",
            'scroll_container': 'div._1r0xc1d'
        }
    ]

    for strategy in selector_strategies:
        print(f"Testing Strategy: {strategy['strategy_name']}...")
        is_valid = test_selectors(driver, strategy)
        if is_valid:
            print(f"✓ Strategy '{strategy['strategy_name']}' passed validation. Using these selectors.")
            return strategy
        else:
            print(f"✗ Strategy '{strategy['strategy_name']}' failed validation.")
            
    print("⚠️ All selector strategies failed. The website structure may have changed significantly.")
    return None

def test_selectors(driver, selectors, max_test=3):
    """
    Tests selectors on the first few cards. Passes if at least one card is parsed successfully.
    """
    try:
        cards = driver.find_elements(By.CSS_SELECTOR, selectors['card'])
        if not cards:
            print("  - Test failed: No cards found with this selector.")
            return False
        
        print(f"  - Found {len(cards)} potential cards. Testing first {max_test}.")

        success_count = 0
        for i, card in enumerate(cards[:max_test]):
            try:
                name = card.find_element(By.CSS_SELECTOR, selectors['name']).text.strip()
                address = card.find_element(By.CSS_SELECTOR, selectors['address']).text.strip()
                
                if name and address:
                    print(f"  - Test on card {i+1} OK: Name='{name}', Address='{address}'")
                    success_count += 1
                else:
                    print(f"  - Test on card {i+1} incomplete: Name or Address is empty (likely an ad).")
            except NoSuchElementException:
                print(f"  - Could not parse card {i+1} with these selectors (likely an ad).")
                continue
        
        if success_count > 0:
            return True
        else:
            print("  - Test failed: Selectors did not extract a complete item from any of the first cards.")
            return False

    except Exception as e:
        print(f"  - An unexpected error occurred during selector testing: {e}")
        return False

# --- Core Parsing & Scraping Functions (Updated) ---

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description='2GIS Parser for Beauty Salons/Barber Shops')
    parser.add_argument('--city', type=str, required=True, help='City to search in (in Russian, e.g., "екатеринбург")')
    parser.add_argument('--segment', type=str, default='салон красоты', help='Business segment to search for')
    parser.add_argument('--limit', type=int, default=100, help='Number of listings to parse')
    parser.add_argument('--output', type=str, default='output.csv', help='Output CSV file name')
    return parser.parse_args()

def init_driver():
    """Initializes the Selenium WebDriver."""
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    return driver

def build_search_url(city, segment):
    """Builds the 2GIS search URL."""
    city_lower = city.lower()
    city_slug = CITY_SLUGS.get(city_lower, quote(city_lower))
    encoded_segment = quote(segment)
    return f'https://2gis.ru/{city_slug}/search/{encoded_segment}'

def handle_cookie_banner(driver):
    """Closes the cookie consent banner if it appears."""
    try:
        cookie_close_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "div._n1367pl > svg"))
        )
        print("Cookie banner found, closing it...")
        cookie_close_button.click()
    except TimeoutException:
        print("Cookie banner not found or already closed.")

def scroll_and_parse(driver, selectors, limit):
    """
    Scrolls the result list to load more companies and then parses them
    using the provided selectors.
    """
    if not selectors:
        print("FATAL: Cannot scroll and parse without valid selectors.")
        return []

    try:
        scroll_container_selector = selectors['scroll_container']
        print(f"Waiting for scroll container '{scroll_container_selector}' to be present...")
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, scroll_container_selector))
        )
        scroll_container = driver.find_element(By.CSS_SELECTOR, scroll_container_selector)
        print("Scroll container found.")
    except TimeoutException:
        print(f"ERROR: Scroll container '{scroll_container_selector}' not found. Aborting scroll.")
        return []

    # Scrolling logic
    previous_card_count = 0
    no_change_strikes = 0
    while no_change_strikes < 5:
        cards = driver.find_elements(By.CSS_SELECTOR, selectors['card'])
        current_card_count = len(cards)
        print(f"Loaded {current_card_count} cards so far...")

        if current_card_count >= limit:
            print(f"Reached target limit of {limit}.")
            break
        if current_card_count == previous_card_count:
            no_change_strikes += 1
        else:
            no_change_strikes = 0
        
        previous_card_count = current_card_count
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scroll_container)
        time.sleep(2)

    # Parsing logic
    print(f"\n--- Parsing {min(previous_card_count, limit)} Companies ---")
    results = []
    all_cards = driver.find_elements(By.CSS_SELECTOR, selectors['card'])
    for i, card in enumerate(all_cards[:limit]):
        name, address, category, rating, url = "", "", "", "", ""
        try:
            name = card.find_element(By.CSS_SELECTOR, selectors['name']).text.strip()
        except NoSuchElementException:
            print(f"  - Card {i+1}: Skipping, no name found.")
            continue
        
        try: address = card.find_element(By.CSS_SELECTOR, selectors['address']).text.strip()
        except: pass
        try: category = card.find_element(By.CSS_SELECTOR, selectors['category']).text.strip()
        except: pass
        try: rating = card.find_element(By.CSS_SELECTOR, selectors['rating']).text.strip()
        except: pass
        try: 
            # Try to find a link within the card for the URL
            link_element = card.find_element(By.TAG_NAME, 'a')
            url = link_element.get_attribute('href')
        except: 
            # If card itself isn't 'a', find inner 'a'
            try:
                inner_link = card.find_element(By.CSS_SELECTOR, "a[href*='/firm/']")
                url = inner_link.get_attribute('href')
            except: pass

        results.append({'name': name, 'address': address, 'category': category, 'rating': rating, 'url': url})
        print(f"  - Parsed {i+1}: {name}")

    return results

# --- Main Execution Logic ---

def main():
    """Main function to orchestrate the parsing process."""
    args = parse_arguments()
    driver = init_driver()
    search_url = build_search_url(args.city, args.segment)
    
    print(f"Navigating to: {search_url}")
    driver.get(search_url)
    
    time.sleep(5)
    handle_cookie_banner(driver)
    time.sleep(3)

    active_selectors = discover_selectors(driver)
    
    results = []
    if active_selectors:
        results = scroll_and_parse(driver, active_selectors, args.limit)
    else:
        print("Could not find any working selectors. Exiting.")

    if results:
        fieldnames = ['name', 'address', 'category', 'rating', 'url']
        print(f"\nSaving {len(results)} results to {args.output}")
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
    else:
        print("No data was parsed.")
    
    print("Closing the browser.")
    driver.quit()

if __name__ == '__main__':
    main()
