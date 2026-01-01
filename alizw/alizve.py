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


def save_checkpoint(page_num, processed_names):
    try:
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                'last_page': page_num,
                'processed_names': list(processed_names)
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
                    'processed_names': set(data.get('processed_names', []))
                }
        except Exception as e:
            logger.error(f"Ошибка загрузки чекпоинта: {e}")
    return {'last_page': 0, 'processed_names': set()}


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
        category = company_element.find_element(By.CSS_SELECTOR, "._4cxmw7").text.strip()
        company_data["Категория"] = category
    except:
        company_data["Категория"] = "Н/Д"

    return company_data


@retry(max_attempts=3, delay=0.2)
def get_company_website(driver, company_url):
    logger.debug(f"Переход на страницу компании: {company_url}")

    main_window = driver.current_window_handle

    try:
        driver.execute_script(f"window.open('{company_url}', '_blank');")
        driver.switch_to.window(driver.window_handles[-1])

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div._qvsf7z"))
        )

        website_script = """
            let website = 'Н/Д';
            
            const contactLinks = document.querySelectorAll('div._172gbf8 div._49kxlr a[href*="http"]');
            for (const link of contactLinks) {
                const href = link.href || '';
                const parent = link.closest('div._172gbf8');
                
                const hasGlobeIcon = parent && parent.querySelector('svg path[d*="M12 4a8 8 0 1 0 8 8"]');
                
                if (hasGlobeIcon && href && !href.includes('tel:') && !href.includes('mailto:')) {
                    website = href;
                    break;
                }
            }
            
            return website;
        """

        website = driver.execute_script(website_script)

        if website and 'link.2gis.ru' in website:
            try:
                redirect_url = website
                driver.execute_script(f"window.open('{redirect_url}', '_blank');")
                driver.switch_to.window(driver.window_handles[-1])
                
                WebDriverWait(driver, 5).until(
                    lambda d: d.current_url != redirect_url
                )
                final_url = driver.current_url
                driver.close()
                driver.switch_to.window(driver.window_handles[-1])
                website = final_url
            except Exception as e:
                logger.debug(f"Не удалось раскрыть редирект: {e}")
                pass

        return website

    except Exception as e:
        logger.error(f"Критическая ошибка при получении веб-сайта: {e}")
        return "Н/Д"

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
                website = get_company_website(driver, link)
                company_data["Веб-сайт"] = website
            except Exception as e:
                logger.error(f"Ошибка при получении веб-сайта для {company_data['Название']}: {e}")
                company_data["Веб-сайт"] = "Н/Д"
        else:
            company_data["Веб-сайт"] = "Н/Д"
        
        # Удаляем временную ссылку 2ГИС
        company_data.pop("Ссылка 2ГИС", None)
        
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


def save_to_csv(data, file_path):
    if not data:
        return

    fieldnames = ["Название", "Категория", "Веб-сайт"]

    for company in data:
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
            "16": ("volgograd", "Волгоград"),
            "17": ("saratov", "Саратов"),
            "18": ("tyumen", "Тюмень"),
            "19": ("tolyatti", "Тольятти"),
            "20": ("izhevsk", "Ижевск"),
            "21": ("barnaul", "Барнаул"),
            "22": ("ulyanovsk", "Ульяновск"),
            "23": ("irkutsk", "Иркутск"),
            "24": ("vladivostok", "Владивосток"),
            "25": ("yaroslavl", "Ярославль"),
            "26": ("habarovsk", "Хабаровск"),
            "27": ("makhachkala", "Махачкала"),
            "28": ("orenburg", "Оренбург"),
            "29": ("novokuznetsk", "Новокузнецк"),
            "30": ("kemerovo", "Кемерово"),
            "31": ("ryazan", "Рязань"),
            "32": ("tomsk", "Томск"),
            "33": ("astrakhan", "Астрахань"),
            "34": ("penza", "Пенза"),
            "35": ("lipetsk", "Липецк"),
            "36": ("tula", "Тула"),
            "37": ("kirov", "Киров"),
            "38": ("cheboksary", "Чебоксары"),
            "39": ("kaliningrad", "Калининград"),
            "40": ("bryanskaya_oblast", "Брянск"),
            "41": ("kursk", "Курск"),
            "42": ("ivanovo", "Иваново"),
            "43": ("magnitogorsk", "Магнитогорск"),
            "44": ("tver", "Тверь"),
            "45": ("stavropol", "Ставрополь"),
            "46": ("simferopol", "Симферополь"),
            "47": ("sevastopol", "Севастополь"),
            "48": ("sochi", "Сочи"),
            "49": ("surgut", "Сургут"),
            "50": ("vologda", "Вологда")
        }

        print("\nДоступные города:")
        for key, (alias, name) in sorted(cities.items(), key=lambda x: int(x[0])):
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
        processed_names = checkpoint['processed_names']

        if current_page > 0:
            logger.info(f"Продолжаем парсинг со страницы {current_page + 1}")
            logger.info(f"Уже обработано {len(processed_names)} уникальных компаний")

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
                    
                    # Проверка на дубликаты по названию
                    company_name = basic_data.get("Название")
                    if company_name in processed_names or company_name == "Н/Д":
                        logger.debug(f"Компания уже обработана или некорректное название: {company_name}")
                        continue
                    
                    companies_basic_data.append(basic_data)
                    processed_names.add(company_name)
                except Exception as e:
                    logger.error(f"Ошибка при извлечении базовых данных для элемента {i}: {e}")
                    continue

            logger.info(f"Извлечены базовые данные для {len(companies_basic_data)} новых компаний")

            if not companies_basic_data:
                logger.info("Нет новых компаний для обработки на этой странице")
                if not go_to_next_page(driver, current_page):
                    logger.info("Достигнута последняя страница")
                    break
                current_page += 1
                save_checkpoint(current_page, processed_names)
                continue

            all_companies_data = process_company_batch_parallel(
                companies_basic_data, 
                driver_pool, 
                max_workers=MAX_WORKERS
            )

            if all_companies_data:
                save_to_csv(all_companies_data, csv_file_path)
                logger.info(f"Данные с страницы {current_page} сохранены в CSV")

            save_checkpoint(current_page, processed_names)

            if not go_to_next_page(driver, current_page):
                logger.info("Достигнута последняя страница результатов")
                break

            current_page += 1

        driver_pool.close_all()
        
        logger.info(f"Парсинг завершен. Обработано {len(processed_names)} уникальных компаний")
        logger.info(f"Данные сохранены в {csv_file_path}")

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
