
import argparse
import csv
import time
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
    """Прокручивает левую панель, чтобы подгрузить больше компаний"""
    results_container = driver.find_element(By.CSS_SELECTOR, "div._1tdquig")
    
    previous_count = 0
    no_change_count = 0
    
    while no_change_count < 5:  # Если 5 раз подряд количество не изменилось - останавливаемся
        # Получаем текущее количество карточек
        cards = driver.find_elements(By.CSS_SELECTOR, "div._1kfg6ff")
        current_count = len(cards)
        
        print(f"Loaded {current_count} companies so far...")
        
        if current_count >= target_count:
            print(f"Reached target: {target_count} companies")
            break
        
        if current_count == previous_count:
            no_change_count += 1
            print(f"No new companies loaded ({no_change_count}/5)")
        else:
            no_change_count = 0
            previous_count = current_count
        
        # Прокручиваем контейнер вниз
        driver.execute_script(
            "arguments[0].scrollTop = arguments[0].scrollHeight", 
            results_container
        )
        time.sleep(2)  # Даём время на подгрузку
    
    final_cards = driver.find_elements(By.CSS_SELECTOR, "div._1kfg6ff")
    print(f"Total companies found after scrolling: {len(final_cards)}")
    return final_cards

def parse_companies(driver, limit):
    """Парсит компании из левой панели с прокруткой"""
    results = []
    
    try:
        # Сначала прокручиваем и загружаем все карточки
        cards = scroll_and_load_more(driver, target_count=limit)
        
        print(f"Starting to parse {min(len(cards), limit)} companies...")
        
        # Парсим каждую карточку
        for i, card in enumerate(cards[:limit]):
            name = None
            try:
                name_el = card.find_element(By.CSS_SELECTOR, "div._zjunba")
                name = name_el.text.strip()
            except:
                print(f"Card {i+1}: No name found, skipping")
                continue
            
            # Остальные поля - опциональные
            address = ""
            try:
                address_el = card.find_element(By.CSS_SELECTOR, "._1p8iqih")
                address = address_el.text.strip()
            except:
                pass
            
            category = ""
            try:
                category_el = card.find_element(By.CSS_SELECTOR, "._1l31g2v")
                category = category_el.text.strip()
            except:
                pass
            
            rating = ""
            try:
                rating_el = card.find_element(By.CSS_SELECTOR, "._v7xwl8")
                rating = rating_el.text.strip()
            except:
                pass
            
            results.append({
                "name": name,
                "address": address,
                "category": category,
                "rating": rating
            })
            
            print(f"Parsed {i+1}/{min(len(cards), limit)}: {name}")
        
        print(f"Successfully parsed {len(results)} companies")
        
    except Exception as e:
        print(f"Error during parsing: {e}")
    
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
        fieldnames = ['name', 'address', 'category', 'rating']
        print(f"Saving {len(results)} results to {args.output}")
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
    
    print("Closing the browser.")
    driver.quit()

if __name__ == '__main__':
    main()
