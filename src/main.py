
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

# Dictionary to map Russian city names to 2GIS URL slugs
CITY_SLUGS = {
    "санкт-петербург": "spb",
    "москва": "moscow",
    "новосибирск": "novosibirsk",
    "екатеринбург": "ekaterinburg",
    "казань": "kazan",
    "нижний новгород": "n_novgorod",
    "красноярск": "krasnoyarsk",
    "челябинск": "chelyabinsk",
    "самара": "samara",
    "уфа": "ufa",
    "краснодар": "krasnodar",
    "омск": "omsk",
    "пермь": "perm",
    "ростов-на-дону": "rostov",
    "воронеж": "voronezh",
    "волгоград": "volgograd",
    "тюмень": "tyumen",
    "сочи": "sochi",
    "тбилиси": "tbilisi"
}

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
    # options.add_argument('--headless') # Uncomment for production runs
    driver = webdriver.Chrome(options=options)
    return driver

def build_search_url(city, segment):
    """Builds the 2GIS search URL with city slug and URL encoding."""
    city_lower = city.lower()
    city_slug = CITY_SLUGS.get(city_lower, quote(city_lower))
    encoded_segment = quote(segment)
    return f'https://2gis.ru/{city_slug}/search/{encoded_segment}'

def handle_cookie_banner(driver):
    """Finds and clicks the cookie consent banner close button if it appears."""
    try:
        cookie_close_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "div._n1367pl > svg"))
        )
        print("Cookie banner found, closing it...")
        cookie_close_button.click()
    except TimeoutException:
        print("Cookie banner not found or already closed.")

def scroll_and_load_more(driver, target_count=100):
    """Прокручивает и загружает карточки"""
    print("Waiting for first cards to appear...")
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/firm/']"))
        )
        print("First cards found!")
    except TimeoutException:
        print("ERROR: No cards found after 20 seconds")
        try:
            page_content = driver.find_element(By.TAG_NAME, "body").text[:1000]
            print(f"Page preview: {page_content}")
        except:
            pass
        return []
    
    results_container = None
    try:
        container_selectors = ["div._1tdquig", "div[class*='search']", "div[class*='results']", "div[class*='list']"]
        for sel in container_selectors:
            try:
                results_container = driver.find_element(By.CSS_SELECTOR, sel)
                print(f"Found scroll container: {sel}")
                break
            except:
                continue
    except:
        print("Container not found, will use window scroll")
    
    previous_count = 0
    no_change_count = 0
    
    while no_change_count < 5:
        cards = driver.find_elements(By.CSS_SELECTOR, "a[href*='/firm/']")
        current_count = len(cards)
        
        print(f"Loaded {current_count} cards (target: {target_count})")
        
        if current_count >= target_count:
            break
        
        if current_count == previous_count:
            no_change_count += 1
        else:
            no_change_count = 0
        
        previous_count = current_count
        
        if results_container:
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", results_container)
        else:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        time.sleep(2)
    
    final_cards = driver.find_elements(By.CSS_SELECTOR, "a[href*='/firm/']")
    print(f"Total cards after scrolling: {len(final_cards)}")
    return final_cards

def parse_companies(driver, limit):
    """Парсит компании из ссылок на фирмы"""
    results = []
    try:
        cards = scroll_and_load_more(driver, target_count=limit)
        if not cards:
            print("ERROR: No cards found!")
            return []
        
        print(f"\nParsing {min(len(cards), limit)} companies...")
        
        for i, card in enumerate(cards[:limit]):
            try:
                name = card.text.strip()
                if not name or len(name) < 2:
                    try:
                        name = card.get_attribute("aria-label") or card.get_attribute("title")
                    except: pass
                
                if not name or len(name) < 2:
                    try:
                        spans = card.find_elements(By.TAG_NAME, "span")
                        for span in spans:
                            text = span.text.strip()
                            if text and len(text) > 2:
                                name = text
                                break
                    except: pass
                
                if not name or len(name) < 2:
                    print(f"Card {i+1}: No valid name found, skipping")
                    continue
                
                firm_url = card.get_attribute("href") or ""
                address, category, rating = "", "", ""
                
                try:
                    parent = card.find_element(By.XPATH, "./ancestor::div[div[contains(@class, '_zjunba')] or div[contains(@class, '_1idnaau')]]")
                    full_text = parent.text
                    lines = [l.strip() for l in full_text.split('\n') if l.strip()]
                    
                    if len(lines) > 1 and lines[0] == name:
                        if len(lines) > 1 and lines[1] != name: category = lines[1]
                        if len(lines) > 2:
                            potential_address = lines[2]
                            if not any(x in potential_address for x in ['оценок', 'отзывов', 'Премия']):
                                address = potential_address

                    rating_match = re.search(r'\d\.\d+', full_text)
                    if rating_match: rating = rating_match.group()
                except: pass
                
                results.append({"name": name, "address": address, "category": category, "rating": rating, "url": firm_url})
                print(f"✓ Parsed {i+1}/{min(len(cards), limit)}: {name}")
                
            except Exception as e:
                print(f"✗ Error parsing card {i+1}: {e}")
                continue
        
        print(f"\n✓ Successfully parsed {len(results)} companies out of {len(cards)} found")
        
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        traceback.print_exc()
    
    return results

def main():
    """Main function to run the parser."""
    args = parse_arguments()
    driver = init_driver()
    search_url = build_search_url(args.city, args.segment)
    
    print(f"Navigating to: {search_url}")
    driver.get(search_url)
    
    time.sleep(5) 
    handle_cookie_banner(driver)
    time.sleep(3)

    results = parse_companies(driver, args.limit)

    if results:
        fieldnames = ['name', 'address', 'category', 'rating', 'url']
        print(f"Saving {len(results)} results to {args.output}")
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
    
    print("Closing the browser.")
    driver.quit()

if __name__ == '__main__':
    main()
