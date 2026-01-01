
import argparse
import csv
import time
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

# --- Global Configuration & Dictionaries ---

CITY_SLUGS = {
    # Original cities
    "санкт-петербург": "spb", "москва": "moscow", "новосибирск": "novosibirsk",
    "екатеринбург": "ekaterinburg", "казань": "kazan", "нижний новгород": "n_novgorod",
    "красноярск": "krasnoyarsk", "челябинск": "chelyabinsk", "самара": "samara",
    "уфа": "ufa", "краснодар": "krasnodar", "омск": "omsk", "пермь": "perm",
    "ростов-на-дону": "rostov", "воронеж": "voronezh", "волгоград": "volgograd",
    "тюмень": "tyumen", "сочи": "sochi", "тбилиси": "tbilisi",
    # Added cities from user feedback
    "саратов": "saratov", "тольятти": "tolyatti", "ижевск": "izhevsk",
    "барнаул": "barnaul", "ульяновск": "ulyanovsk", "иркутск": "irkutsk",
    "владивосток": "vladivostok", "ярославль": "yaroslavl", "хабаровск": "habarovsk",
    "махачкала": "makhachkala", "оренбург": "orenburg", "новокузнецк": "novokuznetsk",
    "кемерово": "kemerovo", "рязань": "ryazan", "томск": "tomsk",
    "астрахань": "astrakhan", "пенза": "penza", "липецк": "lipetsk",
    "тула": "tula", "киров": "kirov", "чебоксары": "cheboksary",
    "калининград": "kaliningrad", "брянск": "bryansk", "курск": "kursk", # Corrected Bryansk
    "иваново": "ivanovo", "магнитогорск": "magnitogorsk", "тверь": "tver"
}

# --- Selector Discovery Functions ---

def discover_selectors(driver):
    print("\n--- Discovering Selectors ---")
    selector_strategies = [
        {
            'strategy_name': 'Modern Div-based Wrapper',
            'card': "div._1kf6gff",
            'name': "div._zjunba",
            'address': "span._sfdp8cg",
            'category': "div._1idnaau",
            'rating': "div._1az2g0c",
        },
    ]
    for strategy in selector_strategies:
        print(f"Testing Strategy: {strategy['strategy_name']}...")
        if test_selectors(driver, strategy):
            print(f"✓ Strategy '{strategy['strategy_name']}' passed validation. Using these selectors.")
            return strategy
        else:
            print(f"✗ Strategy '{strategy['strategy_name']}' failed validation.")
    print("⚠️ All selector strategies failed. The website structure may have changed significantly.")
    return None

def test_selectors(driver, selectors, max_test=5):
    try:
        cards = driver.find_elements(By.CSS_SELECTOR, selectors['card'])
        if not cards:
            print("  - Test failed: No cards found with this selector.")
            return False
        
        print(f"  - Found {len(cards)} potential cards. Testing first {max_test}. ")
        success_count = 0
        for i, card in enumerate(cards[:max_test]):
            try:
                name = card.find_element(By.CSS_SELECTOR, selectors['name']).text.strip()
                if name:
                    print(f"  - Test on card {i+1} OK: Name='{name}'")
                    success_count += 1
            except NoSuchElementException:
                print(f"  - Card {i+1} has no name, likely an ad.")
                continue
        if success_count > 0:
            return True
        else:
            print("  - Test failed: Did not extract a name from any of the first cards.")
            return False
    except Exception as e:
        print(f"  - An unexpected error occurred during selector testing: {e}")
        return False

# --- Core Parsing & Scraping Functions ---

def parse_arguments():
    parser = argparse.ArgumentParser(description='2GIS Parser')
    parser.add_argument('--city', type=str, required=True, help='City to search in (e.g., "екатеринбург")')
    parser.add_argument('--segment', type=str, default='салон красоты', help='Business segment')
    parser.add_argument('--limit', type=int, default=100, help='Number of listings to parse')
    parser.add_argument('--output', type=str, default='output.csv', help='Output CSV file name')
    return parser.parse_args()

def init_driver():
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless') # Run in headless mode for production
    driver = webdriver.Chrome(options=options)
    return driver

def build_search_url(city, segment):
    city_lower = city.lower()
    city_slug = CITY_SLUGS.get(city_lower, quote(city_lower))
    return f'https://2gis.ru/{city_slug}/search/{quote(segment)}'

def handle_cookie_banner(driver):
    try:
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div._n1367pl > svg"))).click()
        print("Cookie banner found and closed.")
    except TimeoutException:
        print("Cookie banner not found or already closed.")

def scroll_and_parse(driver, selectors, limit):
    if not selectors:
        print("FATAL: Cannot scroll and parse without valid selectors.")
        return []

    print("\n--- Loading Companies (will scroll until limit is reached or no new results appear) ---")
    last_valid_count = 0
    no_change_strikes = 0
    
    while True:
        all_cards = driver.find_elements(By.CSS_SELECTOR, selectors['card'])
        valid_cards = [card for card in all_cards if card.find_element(By.CSS_SELECTOR, selectors['name']).text.strip() != ""]
        
        print(f"Found {len(all_cards)} total cards, {len(valid_cards)} are valid companies.")

        if len(valid_cards) >= limit:
            print(f"Reached target of {limit} valid companies.")
            break

        if len(valid_cards) == last_valid_count:
            no_change_strikes += 1
            print(f"Scroll attempt {no_change_strikes}/5: No new valid companies loaded.")
        else:
            no_change_strikes = 0

        if no_change_strikes >= 5:
            print("Stopping scroll: No new companies found after 5 attempts.")
            break
        
        last_valid_count = len(valid_cards)

        try:
            print("  - Scrolling...")
            # Forceful scrolling: scroll last element into view, then scroll window
            driver.execute_script("arguments[0].scrollIntoView(true);", all_cards[-1])
            time.sleep(0.5)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        except (IndexError, StaleElementReferenceException):
            print("  - Could not scroll last card, ending scroll.")
            break
        time.sleep(2.5) # Wait for content to load

    print(f"\n--- Parsing {min(len(valid_cards), limit)} Companies ---")
    results = []
    # We re-fetch the valid cards to be safe
    final_cards = [card for card in driver.find_elements(By.CSS_SELECTOR, selectors['card']) if card.find_element(By.CSS_SELECTOR, selectors['name']).text.strip() != ""]

    for card in final_cards[:limit]:
        name, address, category, rating, url = "", "", "", "", ""
        try:
            name = card.find_element(By.CSS_SELECTOR, selectors['name']).text.strip()
            address = card.find_element(By.CSS_SELECTOR, selectors['address']).text.strip()
            category = card.find_element(By.CSS_SELECTOR, selectors['category']).text.strip()
            rating = card.find_element(By.CSS_SELECTOR, selectors['rating']).text.strip()
            link_element = card.find_element(By.TAG_NAME, 'a')
            url = link_element.get_attribute('href')
            results.append({'name': name, 'address': address, 'category': category, 'rating': rating, 'url': url})
            print(f"✓ Parsed {len(results)}/{limit}: {name}")
        except Exception as e:
            print(f"✗ Could not fully parse a card. Error: {e}")
            
    return results

# --- Main Execution Logic ---

def main():
    args = parse_arguments()
    driver = init_driver()
    search_url = build_search_url(args.city, args.segment)
    
    print(f"Navigating to: {search_url}")
    driver.get(search_url)
    
    handle_cookie_banner(driver)
    time.sleep(1) # Small pause after handling banner

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
        print("Save complete.")
    else:
        print("No data was parsed.")
    
    print("Closing the browser.")
    driver.quit()

if __name__ == '__main__':
    main()
