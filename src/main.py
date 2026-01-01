import argparse
import csv
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description='2GIS Parser for Beauty Salons/Barber Shops')
    parser.add_argument('--city', type=str, required=True, help='City to search in')
    parser.add_argument('--segment', type=str, default='салон красоты', help='Business segment to search for')
    parser.add_argument('--limit', type=int, default=10, help='Number of listings to parse')
    parser.add_argument('--output', type=str, default='results.csv', help='Output CSV file name')
    return parser.parse_args()

def init_driver():
    """Initializes the Selenium WebDriver."""
    options = webdriver.ChromeOptions()
    # The user requested to see the browser
    # options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    return driver

def build_search_url(city, segment):
    """Builds the 2GIS search URL."""
    return f'https://2gis.ru/search/{segment}/rubrics/in/{city}'

def get_company_details(driver):
    """Extracts detailed information from a company page."""
    details = {
        'phones': [],
        'address': 'Н/Д',
        'socials': []
    }
    try:
        # Wait for contact container to be visible
        contact_container = WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, '._172gbf8'))
        )

        # Extract phones
        phone_elements = contact_container.find_elements(By.CSS_SELECTOR, 'a[href^="tel:"]')
        details['phones'] = [elem.get_attribute('href').replace('tel:', '') for elem in phone_elements]

        # Extract address
        try:
            address_element = contact_container.find_element(By.CSS_SELECTOR, '._er2xx9')
            details['address'] = address_element.text
        except NoSuchElementException:
            pass # address not always present in the same way

        # Extract social media links
        social_links_elements = contact_container.find_elements(By.CSS_SELECTOR, 'a._ds2v53')
        for link in social_links_elements:
            href = link.get_attribute('href')
            if href and '2gis.ru' not in href:
                details['socials'].append(href)

    except TimeoutException:
        print("Timed out waiting for company details to load.")
    except Exception as e:
        print(f"An error occurred while extracting details: {e}")

    return details

def calculate_score(data):
    """Calculates a score based on the completeness of the data."""
    score = 0
    if data.get('phones'):
        score += 1
    if data.get('address') != 'Н/Д':
        score += 1
    score += len(data.get('socials', []))
    return score

def main():
    """Main function to run the parser."""
    args = parse_arguments()
    driver = init_driver()
    search_url = build_search_url(args.city, args.segment)
    
    print(f"Navigating to: {search_url}")
    driver.get(search_url)

    results = []
    
    try:
        # Wait for the list of companies to load
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, '_1')]"))
        )
        time.sleep(5) # Allow some extra time for dynamic content to load

        company_elements = driver.find_elements(By.XPATH, "//div[contains(@class, '_1')]")
        
        print(f"Found {len(company_elements)} companies on the first page.")

        for i in range(min(args.limit, len(company_elements))):
            try:
                # Re-find elements to avoid StaleElementReferenceException
                elements = driver.find_elements(By.XPATH, "//div[contains(@class, '_1')]")
                if i >= len(elements):
                    print("Could not re-find element, skipping.")
                    continue
                
                element = elements[i]
                
                # Scroll element into view
                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                time.sleep(3) # Increased wait time

                            company_name = "h3"
                try:
                    name_element = element.find_element(By.CSS_SELECTOR, 'span._owmyyi span')
                    company_name = name_element.text.strip()
                except NoSuchElementException:
                    print("Could not find company name, skipping.")
                    continue

                print(f"Processing ({i+1}/{args.limit}): {company_name}")
                
                try:
                    # Click the element to open details
                    element.click()
                    time.sleep(5) # Wait for details to potentially load in side panel
                except ElementClickInterceptedException:
                    print(f"Could not click element {i} due to interception, skipping.")
                    continue
                except Exception as e:
                    print(f"An error occurred clicking element {i}: {e}, skipping.")
                    continue

                company_details = get_company_details(driver)
                
                data = {
                    'name': company_name,
                    'city': args.city,
                    'phones': ', '.join(company_details['phones']),
                    'address': company_details['address'],
                    'socials': ', '.join(company_details['socials'])
                }
                data['score'] = calculate_score(data)
                
                results.append(data)
                
                # Go back if necessary (or handle single-page app navigation)
                # In modern 2GIS, clicking a list item updates the view, so going back might not be needed
                # if it opens a new page, then: driver.back()

            except Exception as e:
                print(f"Error processing element {i}: {e}")
                # It's good practice to try to go back to the search results
                driver.get(search_url)
                WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, '_1')]"))
                )
                time.sleep(2)


    finally:
        if results:
            print(f"Saving {len(results)} results to {args.output}")
            with open(args.output, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['name', 'city', 'phones', 'address', 'socials', 'score'])
                writer.writeheader()
                writer.writerows(results)
        
        print("Closing the browser.")
        driver.quit()

if __name__ == '__main__':
    main()