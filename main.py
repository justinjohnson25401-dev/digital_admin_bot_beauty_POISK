import os
import time
import csv
import json
import logging
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
from selenium.common.exceptions import StaleElementReferenceException

OUTPUT_FOLDER = "parsed_data"
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'parsing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CHECKPOINT_FILE = os.path.join(OUTPUT_FOLDER, "checkpoint.json")
csv_file_path = None


def retry(max_attempts=3, delay=0.1, backoff=1.5):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = delay
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        logger.error(f"Все {max_attempts} попытки исчерпаны: {e}")
                        raise
                    logger.warning(f"Попытка {attempt}/{max_attempts} не удалась: {e}")
                    time.sleep(current_delay)
                    current_delay *= backoff
            return None
        return wrapper
    return decorator


def wait_for_page_load(driver, timeout=10):
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script('return document.readyState') == 'complete'
    )


def setup_driver():
    logger.info("Инициализация драйвера...")
    chrome_options = Options()

    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-browser-side-navigation")
    chrome_options.add_argument("--disable-features=NetworkService")
    chrome_options.add_argument("--dns-prefetch-disable")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--enable-features=NetworkServiceInProcess")
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    )
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-background-networking")
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-breakpad")
    chrome_options.add_argument("--disable-component-extensions-with-background-pages")
    chrome_options.add_argument("--disable-features=TranslateUI,BlinkGenPropertyTrees")
    chrome_options.add_argument("--disable-ipc-flooding-protection")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--metrics-recording-only")
    chrome_options.add_argument("--mute-audio")
    chrome_options.page_load_strategy = 'eager'

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.cookies": 1,
        "profile.managed_default_content_settings.javascript": 1,
        "profile.managed_default_content_settings.plugins": 1,
        "profile.managed_default_content_settings.popups": 2,
        "profile.managed_default_content_settings.geolocation": 2,
        "profile.managed_default_content_settings.media_stream": 2,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(15)
        driver.set_script_timeout(15)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        logger.info("Драйвер успешно создан")
        return driver
    except Exception as e:
        logger.error(f"Ошибка при создании драйвера: {e}")
        raise


class DriverPool:
    def __init__(self, size=5):
        logger.info(f"Создание пула драйверов размером {size}...")
        self.drivers = []
        for i in range(size):
            try:
                driver = setup_driver()
                self.drivers.append(driver)
                logger.info(f"Драйвер {i+1}/{size} создан")
            except Exception as e:
                logger.error(f"Не удалось создать драйвер {i+1}: {e}")
        
        self.available = Queue()
        for driver in self.drivers:
            self.available.put(driver)
        logger.info(f"Пул драйверов готов: {len(self.drivers)} драйверов")
    
    def get_driver(self):
        return self.available.get()
    
    def return_driver(self, driver):
        self.available.put(driver)
    
    def close_all(self):
        logger.info("Закрытие всех драйверов в пуле...")
        while not self.available.empty():
            driver = self.available.get()
            try:
                driver.quit()
            except:
                pass
        for driver in self.drivers:
            try:
                driver.quit()
            except:
                pass


def save_checkpoint(page_num, processed_urls):
    try:
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                'last_page': page_num,
                'processed_urls': list(processed_urls)
            }, f)
        logger.info(f"Чекпоинт сохранён: страница {page_num}")
    except Exception as e:
        logger.error(f"Ошибка сохранения чекпоинта: {e}")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.info(f"Чекпоинт загружен: страница {data['last_page']}")
                return {
                    'last_page': data.get('last_page', 0),
                    'processed_urls': set(data.get('processed_urls', []))
                }
        except Exception as e:
            logger.error(f"Ошибка загрузки чекпоинта: {e}")
    return {'last_page': 0, 'processed_urls': set()}


def extract_company_basic_data(company_element):
    company_data = {}

    try:
        if not company_element.is_displayed():
            raise Exception("Элемент не отображается")

        name_element = company_element.find_element(By.CSS_SELECTOR, "._1rehek")
        company_data["Название"] = name_element.text.strip()
        company_data["Ссылка 2ГИС"] = name_element.get_attribute("href")
    except Exception as e:
        logger.debug(f"Ошибка при получении названия: {e}")
        company_data["Название"] = "Н/Д"
        company_data["Ссылка 2ГИС"] = "Н/Д"

    try:
        address = company_element.find_element(By.CSS_SELECTOR, "._14quei").text.strip()
        company_data["Адрес"] = address
    except:
        company_data["Адрес"] = "Н/Д"

    try:
        category = company_element.find_element(By.CSS_SELECTOR, "._4cxmw7").text.strip()
        company_data["Категория"] = category
    except:
        company_data["Категория"] = "Н/Д"

    try:
        rating = company_element.find_element(By.CSS_SELECTOR, "._y10azs").text.strip()
        company_data["Рейтинг"] = rating
    except:
        company_data["Рейтинг"] = "Н/Д"

    try:
        reviews = company_element.find_element(By.CSS_SELECTOR, "._jspzdm").text.strip()
        company_data["Отзывы"] = reviews
    except:
        company_data["Отзывы"] = "Н/Д"

    return company_data


@retry(max_attempts=3, delay=0.2)
def get_company_details_optimized(driver, company_url):
    logger.debug(f"Переход на страницу компании: {company_url}")

    main_window = driver.current_window_handle

    try:
        driver.execute_script(f"window.open('{company_url}', '_blank');")
        driver.switch_to.window(driver.window_handles[-1])

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div._qvsf7z"))
        )

        all_data_script = """
            const result = {
                phones: [],
                email: 'Н/Д',
                website: 'Н/Д',
                workingHours: 'Н/Д',
                businessType: 'Н/Д',
                socials: {
                    'ВКонтакте': 'Н/Д',
                    'YouTube': 'Н/Д',
                    'WhatsApp': 'Н/Д',
                    'Telegram': 'Н/Д',
                    'Instagram': 'Н/Д',
                    'Facebook': 'Н/Д',
                    'Одноклассники': 'Н/Д',
                    'Twitter': 'Н/Д',
                    'Другие соцсети': 'Н/Д'
                }
            };

            const phoneElements = document.querySelectorAll('div._b0ke8 a[href^="tel:"]');
            result.phones = Array.from(phoneElements)
                .map(el => el.innerText.trim())
                .filter(text => text);

            const emailElement = document.querySelector('a[href^="mailto:"]');
            if (emailElement) {
                result.email = emailElement.innerText.trim() || emailElement.href.replace('mailto:', '');
            }

            const contactLinks = document.querySelectorAll('div._172gbf8 div._49kxlr a[href*="http"]');
            for (const link of contactLinks) {
                const href = link.href || '';
                const parent = link.closest('div._172gbf8');
                
                const hasGlobeIcon = parent && parent.querySelector('svg path[d*="M12 4a8 8 0 1 0 8 8"]');
                
                if (hasGlobeIcon && href && !href.includes('tel:') && !href.includes('mailto:')) {
                    result.website = href;
                    break;
                }
            }

            const hoursElement = document.querySelector('div._ksc2xc');
            if (hoursElement) {
                const hoursText = hoursElement.innerText.split('\\n')[0].trim();
                if (hoursText) result.workingHours = hoursText;
            }

            const businessTypeButtons = document.querySelectorAll('button._1rehek');
            const businessTypes = [];
            for (const btn of businessTypeButtons) {
                const text = btn.innerText.trim();
                if (text && (text.includes('Интернет-магазин') || text.includes('Розница') || 
                    text.includes('Опт') || text.includes('Производство') || 
                    text.includes('магазин') || text.includes('Шоурум') || text.includes('Салон'))) {
                    businessTypes.push(text);
                }
            }
            if (businessTypes.length > 0) {
                result.businessType = businessTypes.join('; ');
            }

            const socialBlocks = document.querySelectorAll('div._2fgdxvm, div._14uxmys');
            for (const block of socialBlocks) {
                const links = block.querySelectorAll('a[href*="http"]');
                for (const link of links) {
                    const href = link.href || '';
                    const ariaLabel = link.getAttribute('aria-label') || '';
                    const text = link.innerText.trim() || '';
                    
                    if (href.includes('vk.com') || ariaLabel.includes('ВКонтакте')) {
                        result.socials['ВКонтакте'] = href;
                    } else if (href.includes('youtube.com') || href.includes('youtu.be')) {
                        result.socials['YouTube'] = href;
                    } else if (href.includes('wa.me') || href.includes('whatsapp') || ariaLabel.includes('WhatsApp')) {
                        result.socials['WhatsApp'] = href;
                    } else if (href.includes('t.me') || href.includes('telegram') || ariaLabel.includes('Telegram')) {
                        result.socials['Telegram'] = href;
                    } else if (href.includes('instagram.com')) {
                        result.socials['Instagram'] = href;
                    } else if (href.includes('facebook.com') || href.includes('fb.com')) {
                        result.socials['Facebook'] = href;
                    } else if (href.includes('ok.ru') || ariaLabel.includes('Одноклассники')) {
                        result.socials['Одноклассники'] = href;
                    } else if (href.includes('twitter.com') || href.includes('x.com')) {
                        result.socials['Twitter'] = href;
                    }
                }
            }

            const allSocialLinks = document.querySelectorAll('a[href*="http"]');
            for (const link of allSocialLinks) {
                const href = link.href || '';
                if (!href) continue;

                const hasSocialIcon = link.querySelector('svg[fill="#028eff"], svg path[fill-rule="evenodd"]');
                
                if (hasSocialIcon || link.closest('div._2fgdxvm')) {
                    if ((href.includes('vk.com') || href.includes('vkontakte')) && result.socials['ВКонтакте'] === 'Н/Д') {
                        result.socials['ВКонтакте'] = href;
                    } else if ((href.includes('youtube.com') || href.includes('youtu.be')) && result.socials['YouTube'] === 'Н/Д') {
                        result.socials['YouTube'] = href;
                    } else if ((href.includes('wa.me') || href.includes('whatsapp')) && result.socials['WhatsApp'] === 'Н/Д') {
                        result.socials['WhatsApp'] = href;
                    } else if ((href.includes('t.me') || href.includes('telegram')) && result.socials['Telegram'] === 'Н/Д') {
                        result.socials['Telegram'] = href;
                    } else if (href.includes('instagram.com') && result.socials['Instagram'] === 'Н/Д') {
                        result.socials['Instagram'] = href;
                    } else if ((href.includes('facebook.com') || href.includes('fb.com')) && result.socials['Facebook'] === 'Н/Д') {
                        result.socials['Facebook'] = href;
                    } else if (href.includes('ok.ru') && result.socials['Одноклассники'] === 'Н/Д') {
                        result.socials['Одноклассники'] = href;
                    } else if ((href.includes('twitter.com') || href.includes('x.com')) && result.socials['Twitter'] === 'Н/Д') {
                        result.socials['Twitter'] = href;
                    }
                }
            }

            return result;
        """

        data = driver.execute_script(all_data_script)

        if not data.get('phones'):
            try:
                show_button = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button._1tkj2hw"))
                )
                driver.execute_script("arguments[0].click();", show_button)
                
                WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div._b0ke8 a[href^='tel:']"))
                )
                
                phone_elements = driver.find_elements(By.CSS_SELECTOR, "div._b0ke8 a[href^='tel:']")
                phones = [el.text.strip() for el in phone_elements if el.text.strip()]
                data['phones'] = phones
            except:
                pass

        if data.get('website') and 'link.2gis.ru' in data['website']:
            try:
                redirect_url = data['website']
                driver.execute_script(f"window.open('{redirect_url}', '_blank');")
                driver.switch_to.window(driver.window_handles[-1])
                
                WebDriverWait(driver, 5).until(
                    lambda d: d.current_url != redirect_url
                )
                final_url = driver.current_url
                driver.close()
                driver.switch_to.window(driver.window_handles[-1])
                data['website'] = final_url
            except Exception as e:
                logger.debug(f"Не удалось раскрыть редирект: {e}")
                pass

        detailed_info = {
            "Телефоны": "; ".join(data['phones']) if data['phones'] else "Н/Д",
            "Email": data.get('email', 'Н/Д'),
            "Веб-сайт": data['website'],
            "Режим работы": data['workingHours'],
            "Тип предприятия": data['businessType']
        }
        
        detailed_info.update(data['socials'])

        return detailed_info

    except Exception as e:
        logger.error(f"Критическая ошибка при получении данных компании: {e}")
        return {
            "Телефоны": "Н/Д",
            "Email": "Н/Д",
            "Веб-сайт": "Н/Д",
            "Режим работы": "Н/Д",
            "Тип предприятия": "Н/Д",
            "ВКонтакте": "Н/Д",
            "YouTube": "Н/Д",
            "WhatsApp": "Н/Д",
            "Telegram": "Н/Д",
            "Instagram": "Н/Д",
            "Facebook": "Н/Д",
            "Одноклассники": "Н/Д",
            "Twitter": "Н/Д",
            "Другие соцсети": "Н/Д"
        }

    finally:
        try:
            driver.close()
            driver.switch_to.window(main_window)
        except Exception as e:
            logger.debug(f"Ошибка при закрытии вкладки: {e}")


def process_single_company(company_basic_data, driver_pool):
    driver = driver_pool.get_driver()
    
    try:
        company_data = company_basic_data.copy()
        
        link = company_data.get("Ссылка 2ГИС")
        if link and link != "Н/Д":
            try:
                detailed_info = get_company_details_optimized(driver, link)
                
                if detailed_info.get("Веб-сайт") and detailed_info.get("Веб-сайт") != "Н/Д":
                    company_data["Ссылка"] = detailed_info["Веб-сайт"]
                else:
                    company_data["Ссылка"] = link
                
                company_data.update(detailed_info)
            except Exception as e:
                logger.error(f"Ошибка при получении деталей для {company_data['Название']}: {e}")
                company_data["Ссылка"] = link
        else:
            company_data["Ссылка"] = "Н/Д"
        
        logger.info(f"Обработана компания: {company_data['Название']}")
        return company_data
        
    except Exception as e:
        logger.error(f"Ошибка при обработке компании: {e}")
        return None
    finally:
        driver_pool.return_driver(driver)


def process_company_batch_parallel(companies_basic_data, driver_pool, max_workers=5):
    companies_data = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_single_company, company_data, driver_pool): company_data
            for company_data in companies_basic_data
        }
        
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    companies_data.append(result)
            except Exception as e:
                logger.error(f"Ошибка при получении результата: {e}")
    
    return companies_data


def determine_work_mode(business_type):
    if business_type == "Н/Д":
        return "Н/Д"

    business_type = business_type.lower()

    online_indicators = ["интернет-магазин", "интернет магазин", "онлайн"]
    offline_indicators = ["розница", "опт", "оптовая", "производство", 
                         "магазин", "шоурум", "салон", "студия", "офис"]

    has_online = any(indicator in business_type for indicator in online_indicators)
    has_offline = any(indicator in business_type for indicator in offline_indicators)

    if has_online and has_offline:
        return "Онлайн/Оффлайн"
    elif has_online:
        return "Онлайн"
    elif has_offline:
        return "Оффлайн"
    else:
        return "Не определено"


def save_to_csv(data, file_path):
    if not data:
        return

    fieldnames = [
        "Название", "Адрес", "Категория", "Рейтинг", "Отзывы", "Ссылка",
        "Телефоны", "Email", "Веб-сайт", "Режим работы", "Режим работы (тип)", 
        "Тип предприятия", "ВКонтакте", "YouTube", "WhatsApp", "Telegram", 
        "Instagram", "Facebook", "Одноклассники", "Twitter", "Другие соцсети", 
        "Ссылка 2ГИС"
    ]

    for company in data:
        company["Режим работы (тип)"] = determine_work_mode(
            company.get("Тип предприятия", "Н/Д")
        )
        for field in fieldnames:
            company.setdefault(field, "Н/Д")

    file_exists = os.path.isfile(file_path)

    try:
        with open(file_path, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
            if not file_exists:
                writer.writeheader()
            writer.writerows(data)
        logger.info(f"Сохранено {len(data)} записей в {file_path}")
    except Exception as e:
        logger.error(f"Ошибка сохранения в CSV: {e}")


def go_to_next_page(driver, current_page):
    next_page_num = current_page + 1
    
    try:
        next_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//span[contains(@class, '_19xy60y') and text()='{next_page_num}']")
            )
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
        time.sleep(0.1)
        driver.execute_script("arguments[0].click();", next_button)
        wait_for_page_load(driver, timeout=10)
        time.sleep(0.3)
        return True
    except:
        pass
    
    try:
        next_arrow = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(@aria-label, 'Следующ')]")
            )
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", next_arrow)
        time.sleep(0.1)
        driver.execute_script("arguments[0].click();", next_arrow)
        wait_for_page_load(driver, timeout=10)
        time.sleep(0.3)
        return True
    except:
        pass
    
    try:
        show_more = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(), 'Показать ещё')]")
            )
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", show_more)
        time.sleep(0.1)
        driver.execute_script("arguments[0].click();", show_more)
        wait_for_page_load(driver, timeout=10)
        time.sleep(0.3)
        return True
    except:
        pass
    
    return False


def main():
    global csv_file_path
    
    try:
        logger.info("=== Настройка парсинга 2ГИС ===")

        cities = {
            "1": ("spb", "Санкт-Петербург"),
            "2": ("moscow", "Москва"),
            "3": ("novosibirsk", "Новосибирск"),
            "4": ("ekaterinburg", "Екатеринбург"),
            "5": ("kazan", "Казань"),
            "6": ("n_novgorod", "Нижний Новгород"),
            "7": ("krasnoyarsk", "Красноярск"),
            "8": ("chelyabinsk", "Челябинск"),
            "9": ("samara", "Самара"),
            "10": ("ufa", "Уфа"),
            "11": ("krasnodar", "Краснодар"),
            "12": ("omsk", "Омск"),
            "13": ("perm", "Пермь"),
            "14": ("rostov", "Ростов-на-Дону"),
            "15": ("voronezh", "Воронеж"),
            "16": ("volgograd", "Волгоград")
        }

        print("\nДоступные города:")
        for key, (alias, name) in cities.items():
            print(f"{key}. {name}")

        while True:
            city_choice = input("\nВыберите город (введите номер): ").strip()
            if city_choice in cities:
                city_alias, city_name = cities[city_choice]
                break
            else:
                print("Неверный выбор. Попробуйте снова.")

        search_query = input(f"\nВведите поисковый запрос для {city_name}: ").strip()

        if not search_query:
            search_query = "детская мебель"
            logger.info(f"Используется запрос по умолчанию: '{search_query}'")

        safe_query = "".join(c for c in search_query if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_city = city_name.replace("-", "_").replace(" ", "_")
        csv_file_path = os.path.join(OUTPUT_FOLDER, f"{safe_city}_{safe_query.replace(' ', '_')}.csv")

        logger.info(f"Начинаем парсинг:")
        logger.info(f"Город: {city_name}")
        logger.info(f"Запрос: {search_query}")
        logger.info(f"Файл результатов: {csv_file_path}")

        checkpoint = load_checkpoint()
        current_page = checkpoint['last_page']
        processed_urls = checkpoint['processed_urls']

        if current_page > 0:
            logger.info(f"Продолжаем парсинг со страницы {current_page + 1}")

        driver = setup_driver()

        MAX_WORKERS = 5

        driver_pool = DriverPool(MAX_WORKERS)

        logger.info("Открытие сайта 2ГИС...")
        driver.get(f"https://2gis.ru/{city_alias}")
        wait_for_page_load(driver, timeout=10)
        time.sleep(0.5)
        logger.info(f"Открыт 2ГИС для города {city_name}")

        for attempt in range(3):
            try:
                search_input = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input._cu5ae4"))
                )
                search_input.clear()
                search_input.send_keys(search_query)
                search_input.send_keys(Keys.ENTER)
                logger.info(f"Введен запрос: '{search_query}'")
                break
            except StaleElementReferenceException:
                if attempt == 2:
                    raise
                time.sleep(0.3)
                continue

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div._1kf6gff"))
        )

        time.sleep(0.5)

        if current_page > 0:
            logger.info(f"Навигация на страницу {current_page}...")
            for page in range(1, current_page + 1):
                if not go_to_next_page(driver, page):
                    logger.warning(f"Не удалось перейти на страницу {page + 1}")
                    break
                logger.info(f"Переход на страницу {page + 1}")

        current_page = max(1, current_page)
        max_pages = 100

        while current_page <= max_pages:
            logger.info(f"Обработка страницы {current_page}")

            time.sleep(0.3)

            company_elements = driver.find_elements(By.CSS_SELECTOR, "div._1kf6gff")

            if not company_elements:
                logger.warning("Компании не найдены на этой странице")
                break

            logger.info(f"Найдено {len(company_elements)} компаний на странице")

            companies_basic_data = []
            for i, element in enumerate(company_elements):
                try:
                    try:
                        current_elements = driver.find_elements(By.CSS_SELECTOR, "div._1kf6gff")
                        if i < len(current_elements):
                            element = current_elements[i]
                        else:
                            logger.debug(f"Элемент {i} больше не доступен")
                            continue
                    except:
                        logger.debug(f"Не удалось повторно найти элемент {i}")
                        continue

                    basic_data = extract_company_basic_data(element)
                    
                    if basic_data.get("Ссылка 2ГИС") in processed_urls:
                        logger.debug(f"Компания уже обработана: {basic_data.get('Название')}")
                        continue
                    
                    companies_basic_data.append(basic_data)
                    processed_urls.add(basic_data.get("Ссылка 2ГИС"))
                except Exception as e:
                    logger.error(f"Ошибка при извлечении базовых данных для элемента {i}: {e}")
                    continue

            logger.info(f"Извлечены базовые данные для {len(companies_basic_data)} компаний")

            if not companies_basic_data:
                logger.info("Нет новых компаний для обработки на этой странице")
                if not go_to_next_page(driver, current_page):
                    logger.info("Достигнута последняя страница")
                    break
                current_page += 1
                save_checkpoint(current_page, processed_urls)
                continue

            all_companies_data = process_company_batch_parallel(
                companies_basic_data, 
                driver_pool, 
                max_workers=MAX_WORKERS
            )

            if all_companies_data:
                save_to_csv(all_companies_data, csv_file_path)
                logger.info(f"Данные с страницы {current_page} сохранены в CSV")

            save_checkpoint(current_page, processed_urls)

            if not go_to_next_page(driver, current_page):
                logger.info("Достигнута последняя страница результатов")
                break

            current_page += 1

        driver_pool.close_all()
        
        logger.info(f"Парсинг завершен. Данные сохранены в {csv_file_path}")

        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            logger.info("Чекпоинт удален после успешного завершения")

    except Exception as e:
        logger.error(f"Произошла критическая ошибка: {e}", exc_info=True)

    finally:
        try:
            driver.quit()
        except:
            pass
        try:
            driver_pool.close_all()
        except:
            pass


if __name__ == "__main__":
    main()