from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import random
import zipfile
import re
import concurrent.futures
from functools import partial
import threading
import pymysql
import base64
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from collections import deque

MAX_WORKERS = 1

# Add this global variable
total_bandwidth = 0
bandwidth_lock = threading.Lock()

# New global variables
scrape_count = 0
scrape_times = deque(maxlen=10)  # Store the last 10 scrape times
scrape_lock = threading.Lock()

def get_proxies_from_file(file_path):
    with open(file_path, 'r') as f:
        proxies = f.readlines()
    return [proxy.strip() for proxy in proxies]

def get_random_proxy(proxies):
    return random.choice(proxies)

def create_proxy_extension(proxy):
    """Create a Chrome extension to handle proxy with authentication."""
    proxy_host, proxy_port, proxy_user, proxy_pass = proxy.split(':')

    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Chrome Proxy",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        },
        "minimum_chrome_version":"22.0.0"
    }
    """

    background_js = f"""
    var config = {{
            mode: "fixed_servers",
            rules: {{
              singleProxy: {{
                scheme: "http",
                host: "{proxy_host}",
                port: parseInt({proxy_port})
              }},
              bypassList: ["localhost"]
            }}
          }};
    chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});
    function callbackFn(details) {{
        return {{
            authCredentials: {{
                username: "{proxy_user}",
                password: "{proxy_pass}"
            }}
        }};
    }}
    chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {{urls: ["<all_urls>"]}},
                ['blocking']
    );
    """

    pluginfile = '/tmp/proxy_auth_plugin.zip'

    with zipfile.ZipFile(pluginfile, 'w') as zp:
        zp.writestr("manifest.json", manifest_json)
        zp.writestr("background.js", background_js)

    return pluginfile

def setup_driver(proxy):
    # Set up Chrome options
    chrome_options = Options()
    # chrome_options.add_argument("--headless")  # Run in headless mode

    # Create and add proxy authentication extension
    proxy_extension = create_proxy_extension(proxy)
    chrome_options.add_extension(proxy_extension)

    # Use webdriver_manager to manage ChromeDriver path
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    return driver

def parse_meta_tags(meta_tags):
    data = {}
    for tag in meta_tags:
        if tag.get('property') == 'og:description':
            content = tag.get('content', '')
            match = re.match(r'([\d,]+\.?\d*[KM]?) Followers, ([\d,]+) Following, ([\d,]+) Posts', content)
            if match:
                data['followers'] = match.group(1)
                data['following'] = match.group(2)
                data['posts'] = match.group(3)
        elif tag.get('name') == 'description':
            content = tag.get('content', '')
            # Correct the regex pattern to extract the bio accurately
            bio_match = re.search(r'on Instagram: "(.*?)"', content, re.DOTALL)
            if bio_match:
                data['bio'] = bio_match.group(1)
    return data

def update_bandwidth(response_size):
    global total_bandwidth
    with bandwidth_lock:
        total_bandwidth += response_size

def create_database():
    conn = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='password',
        database='main'
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute('''CREATE TABLE IF NOT EXISTS instagram_profiles
                             (username VARCHAR(255) PRIMARY KEY,
                              followers VARCHAR(255),
                              following VARCHAR(255),
                              posts VARCHAR(255),
                              bio TEXT)''')
        conn.commit()
    finally:
        conn.close()

def update_scrape_stats(scrape_time):
    global scrape_count
    with scrape_lock:
        scrape_count += 1
        scrape_times.append(scrape_time)

def print_scrape_stats():
    with scrape_lock:
        if scrape_times:
            avg_time = sum(scrape_times) / len(scrape_times)
            scrapes_per_minute = 60 / avg_time if avg_time > 0 else 0
            print(f"\rScrapes: {scrape_count}, Speed: {scrapes_per_minute:.2f} scrapes/minute", end="", flush=True)

def scrape_instagram_head(username, proxies, max_retries=3):
    start_time = time.time()
    for attempt in range(max_retries):
        try:
            url = f"https://www.instagram.com/{username}/"
            proxy = get_random_proxy(proxies)
            print(f"Attempt {attempt + 1} for {username} using proxy: {proxy}")
            driver = setup_driver(proxy)
            
            driver.get(url)
            
            # Wait for the meta tags to be present
            wait = WebDriverWait(driver, 10)  # Maximum wait time of 10 seconds
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "meta")))
            
            # Get page size and update bandwidth
            page_size = len(driver.page_source.encode('utf-8'))
            update_bandwidth(page_size)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            meta_tags = soup.find_all('meta')
            
            if meta_tags:
                parsed_data = parse_meta_tags(meta_tags)
                parsed_data['username'] = username
                
                # Check if all required keys are present
                required_keys = ['username', 'followers', 'following', 'posts']
                if all(key in parsed_data for key in required_keys):
                    # Save to database
                    conn = pymysql.connect(
                        host='127.0.0.1',
                        user='root',
                        password='password',
                        database='main'
                    )
                    try:
                        with conn.cursor() as cursor:
                            cursor.execute('''INSERT INTO instagram_profiles
                                             (username, followers, following, posts, bio)
                                             VALUES (%s, %s, %s, %s, %s)
                                             ON DUPLICATE KEY UPDATE
                                             followers = VALUES(followers),
                                             following = VALUES(following),
                                             posts = VALUES(posts),
                                             bio = VALUES(bio)''',
                                          (parsed_data['username'],
                                           parsed_data['followers'],
                                           parsed_data['following'],
                                           parsed_data['posts'],
                                           parsed_data.get('bio', '')))
                        conn.commit()
                    finally:
                        conn.close()
                    
                    scrape_time = time.time() - start_time
                    update_scrape_stats(scrape_time)
                    print_scrape_stats()
                    
                    print(f"\nData for {username} saved to database, {page_size} bytes, {total_bandwidth / (1024 * 1024):.2f} MB, data: {parsed_data}")
                    return parsed_data
                else:
                    missing_keys = [key for key in required_keys if key not in parsed_data]
                    print(f"Missing required keys for {username}: {missing_keys}")
                    raise Exception(f"Missing required keys: {missing_keys}")
            else:
                print(f"No meta tags found for {username}")
                raise Exception("No meta tags found")
        except Exception as e:
            print(f"An error occurred while scraping {username}: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying... (Attempt {attempt + 2}/{max_retries})")
                time.sleep(random.uniform(1, 3))  # Add a random delay between retries
            else:
                print(f"Max retries reached for {username}. Moving to the next username.")
        finally:
            if 'driver' in locals():
                driver.quit()
    
    scrape_time = time.time() - start_time
    update_scrape_stats(scrape_time)
    print_scrape_stats()
    return None

def get_usernames_from_file(file_path):
    with open(file_path, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def main():
    proxies_file_path = "Webshare 10 proxies.txt"
    usernames_file_path = "usernames.txt"
    
    create_database()
    
    proxies = get_proxies_from_file(proxies_file_path)
    usernames = get_usernames_from_file(usernames_file_path)
    
    # Use partial to create a function with fixed proxies argument
    scrape_func = partial(scrape_instagram_head, proxies=proxies)
    
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(scrape_func, usernames))
    
    end_time = time.time()
    total_time = end_time - start_time
    
    successful = [result for result in results if result]
    failed = len(usernames) - len(successful)
    
    print(f"\n\nScraping completed.")
    print(f"Total scrapes: {scrape_count}")
    print(f"Successfully scraped: {len(successful)} profiles")
    print(f"Failed to scrape: {failed} profiles")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Average speed: {(scrape_count / total_time) * 60:.2f} scrapes/minute")
    print(f"Total bandwidth used: {total_bandwidth / (1024 * 1024):.2f} MB")

if __name__ == "__main__":
    main()