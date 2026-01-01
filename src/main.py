
import argparse
import csv
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description='2GIS Parser for Beauty Salons/Barber Shops')
    parser.add_argument('--city', type=str, required=True, help='City to search in')
    parser.add_argument('--segment', type=str, default='салон красоты', help='Business segment to search for')
    parser.add_argument('--limit', type=int, default=10, help='Number of listings to parse')
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
    return f'https://2gis.ru/search/{segment}/rubrics/in/{city}'

def parse_companies(driver, limit):
    """Parses company data from the left panel."""
    results = []
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div._1kfg6ff"))
        )
        cards = driver.find_elements(By.CSS_SELECTOR, "div._1kfg6ff")
        print(f"Found {len(cards)} companies on the first page.")

        for i, card in enumerate(cards[:limit]):
            name, address, category, rating = None, None, None, ""
            try:
                name_el = card.find_element(By.CSS_SELECTOR, "div._zjunba")
                name = name_el.text.strip()

                address_el = card.find_element(By.CSS_SELECTOR, "._1p8iqih")
                address = address_el.text.strip()

                category_el = card.find_element(By.CSS_SELECTOR, "._1l31g2v")
                category = category_el.text.strip()

                try:
                    rating_el = card.find_element(By.CSS_SELECTOR, "._v7xwl8")
                    rating = rating_el.text.strip()
                except NoSuchElementException:
                    rating = ""

                if name:
                    results.append({
                        'name': name,
                        'address': address,
                        'category': category,
                        'rating': rating
                    })

            except Exception as e:
                print(f"Error parsing card {i}: {e}")
                continue
    
    except TimeoutException:
        print("Timed out waiting for company cards to load.")

    return results

def main():
    """Main function to run the parser."""
    args = parse_arguments()
    driver = init_driver()
    search_url = build_search_url(args.city, args.segment)
    
    print(f"Navigating to: {search_url}")
    driver.get(search_url)

    results = parse_companies(driver, args.limit)
    
    successfully_parsed = sum(1 for r in results if r.get('name'))
    print(f"Successfully parsed {successfully_parsed} companies.")

    if results:
        print(f"Saving {len(results)} results to {args.output}")
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['name', 'address', 'category', 'rating'])
            writer.writeheader()
            writer.writerows(results)
    
    print("Closing the browser.")
    driver.quit()

if __name__ == '__main__':
    main()
