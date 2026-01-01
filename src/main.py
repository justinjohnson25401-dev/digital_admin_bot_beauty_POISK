
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
    parser.add_argument('--limit', type=int, default=10, help='Number of listings to parse')
    parser.add_argument('--output', type=str, default='output.csv', help='Output CSV file name')
    return parser.parse_args()

def init_driver():
    """Initializes the Selenium WebDriver."""
    options = webdriver.ChromeOptions()
    # Use headless mode for environments without a display
    # options.add_argument('--headless')
    # options.add_argument('--no-sandbox')
    # options.add_argument('--disable-dev-shm-usage')
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
            EC.element_to_be_clickable((By.CSS_SELECTOR, "div._1x5s866 > svg"))
        )
        print("Cookie banner found, closing it...")
        cookie_close_button.click()
    except TimeoutException:
        print("Cookie banner not found or already closed.")

def parse_companies(driver, limit):
    """Parses company data from the search results using updated selectors."""
    results = []
    try:
        print("Waiting for company cards to appear...")
        # UPDATED SELECTOR for the list of result cards
        card_selector = "div._1hf7139"
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, card_selector))
        )
        cards = driver.find_elements(By.CSS_SELECTOR, card_selector)
        print(f"Found {len(cards)} companies on the page.")

        for i, card in enumerate(cards[:limit]):
            name, address, rating = "Н/Д", "Н/Д", "Н/Д"
            try:
                # UPDATED SELECTOR for the company name
                name = card.find_element(By.CSS_SELECTOR, "span._1al0wlf").text.strip()
                
                # UPDATED SELECTOR for the address
                address = card.find_element(By.CSS_SELECTOR, "div._j1es63").text.strip()
                
                # UPDATED SELECTOR for rating (optional)
                try:
                    rating = card.find_element(By.CSS_SELECTOR, "span._158oqdp").text.strip()
                except NoSuchElementException:
                    rating = "" # No rating found

                if name and name != "Н/Д":
                    # The 'category' field is removed for now as it's not reliably present
                    results.append({'name': name, 'address': address, 'rating': rating})

            except Exception as e:
                print(f"Error parsing card #{i+1}: {e}")
    except TimeoutException:
        print("Timed out: Company cards not found. The page structure might have changed again.")
    return results

def main():
    """Main function to run the parser."""
    args = parse_arguments()
    driver = init_driver()
    search_url = build_search_url(args.city, args.segment)
    
    print(f"Navigating to: {search_url}")
    driver.get(search_url)
    
    # Wait for page load and potential cookie banner
    time.sleep(3) 
    handle_cookie_banner(driver)
    time.sleep(3) # A small extra delay for safety

    results = parse_companies(driver, args.limit)
    
    print(f"Successfully parsed {len(results)} companies.")

    if results:
        fieldnames = ['name', 'address', 'rating'] # Removed 'category'
        print(f"Saving {len(results)} results to {args.output}")
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
    
    print("Closing the browser.")
    driver.quit()

if __name__ == '__main__':
    main()
