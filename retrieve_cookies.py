import requests
import time
import json
import random
import codecs
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
from selenium.common.exceptions import WebDriverException
import os
import zipfile

MOBILE_USER_AGENTS = [
    'Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 9; Pixel 3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 8.0.0; SM-G950F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36'
]

def create_proxy_extension(proxy):
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

    background_js = """
    var config = {
            mode: "fixed_servers",
            rules: {
              singleProxy: {
                scheme: "http",
                host: "%s",
                port: parseInt(%s)
              },
              bypassList: ["localhost"]
            }
          };

    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

    function callbackFn(details) {
        return {
            authCredentials: {
                username: "%s",
                password: "%s"
            }
        };
    }

    chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {urls: ["<all_urls>"]},
                ['blocking']
    );
    """ % (proxy.split(':')[0], proxy.split(':')[1], proxy.split(':')[2], proxy.split(':')[3])

    extension_dir = 'proxy_auth_extension'
    if not os.path.exists(extension_dir):
        os.mkdir(extension_dir)

    with open(f'{extension_dir}/manifest.json', 'w') as f:
        f.write(manifest_json)

    with open(f'{extension_dir}/background.js', 'w') as f:
        f.write(background_js)

    with zipfile.ZipFile('proxy_auth_extension.zip', 'w') as zp:
        zp.write(f'{extension_dir}/manifest.json', 'manifest.json')
        zp.write(f'{extension_dir}/background.js', 'background.js')

    return os.path.abspath('proxy_auth_extension.zip')

def create_fortified_browser(proxy, user_agent):
    options = Options()
    options.add_argument(f'user-agent={user_agent}')
    
    if proxy:
        proxy_extension = create_proxy_extension(proxy)
        options.add_extension(proxy_extension)
    
    # Additional options to make the browser more stealthy
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(options=options)
    
    # Apply selenium-stealth
    stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )
    
    return driver

def login_to_instagram(username, password, proxy, user_agent):
    session = requests.Session()
    
    # Set up the proxy
    if proxy:
        # Parse the proxy string
        proxy_parts = proxy.split(':')
        if len(proxy_parts) == 4:  # Format: hostname:port:username:password
            proxy_url = f"http://{proxy_parts[2]}:{proxy_parts[3]}@{proxy_parts[0]}:{proxy_parts[1]}"
        else:
            proxy_url = f"http://{proxy}"
        
        proxies = {
            'http': proxy_url,
            'https': proxy_url
        }
        session.proxies.update(proxies)
    
    # Instagram login URL
    login_url = 'https://www.instagram.com/accounts/login/ajax/'
    
    # Headers to mimic a browser
    headers = {
        'User-Agent': user_agent,
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://www.instagram.com/accounts/login/',
        'x-csrftoken': 'missing',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://www.instagram.com',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }
    
    # Get the CSRF token
    try:
        response = session.get('https://www.instagram.com/accounts/login/', headers=headers)
        csrf_token = response.cookies.get('csrftoken', 'missing')
    except requests.exceptions.RequestException as e:
        print(f"Error getting CSRF token: {e}")
        return None
    
    # Prepare login data
    login_data = {
        'username': username,
        'enc_password': f'#PWD_INSTAGRAM_BROWSER:0:{int(time.time())}:{password}',
        'queryParams': {},
        'optIntoOneTap': 'false'
    }
    
    # Update headers with CSRF token
    headers['x-csrftoken'] = csrf_token
    
    # Attempt login
    try:
        login_response = session.post(login_url, data=login_data, headers=headers)
        print(f"Response status code: {login_response.status_code}")
        print(f"Response headers: {login_response.headers}")
        print(f"Response content: {login_response.text}")

        if login_response.status_code == 200 or login_response.status_code == 400:
            try:
                response_json = login_response.json()
                if response_json.get('authenticated'):
                    print("Login successful!")
                    cookies = session.cookies.get_dict()
                    cookie_string = '; '.join([f"{k}={v}" for k, v in cookies.items()])
                    return cookie_string
                elif 'checkpoint_required' in response_json or 'checkpoint_url' in response_json:
                    print("Checkpoint required. Opening browser for manual intervention.")
                    checkpoint_url = response_json.get('checkpoint_url')
                    full_checkpoint_url = f"https://www.instagram.com{checkpoint_url}"
                    
                    # Create and use fortified browser
                    driver = create_fortified_browser(proxy, user_agent)
                    driver.get(full_checkpoint_url)
                    
                    print("Please complete the checkpoint in the opened browser window.")
                    input("Press Enter after completing the checkpoint...")
                    
                    # Wait for Instagram to redirect after checkpoint (adjust timeout as needed)
                    WebDriverWait(driver, 30).until(EC.url_contains("instagram.com/"))
                    
                    # Get cookies from the browser
                    browser_cookies = driver.get_cookies()
                    driver.quit()
                    
                    # Convert browser cookies to a string
                    cookie_string = '; '.join([f"{cookie['name']}={cookie['value']}" for cookie in browser_cookies])
                    
                    if cookie_string:
                        print("Cookies retrieved after checkpoint.")
                        return cookie_string
                    else:
                        print("Failed to retrieve cookies after checkpoint.")
                        return None
                else:
                    print("Login failed.")
                    return None
            except json.JSONDecodeError:
                print("Failed to decode JSON response.")
                return None
        else:
            print(f"Login failed with status code: {login_response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error during login request: {e}")
        return None
    except WebDriverException as e:
        print(f"Error with WebDriver: {e}")
        return None

def retrieve_cookies_from_json():
    try:
        # Try different encodings
        encodings = ['utf-8', 'utf-8-sig', 'utf-16', 'ascii']
        content = None
        
        for encoding in encodings:
            try:
                with codecs.open('Files/instagram_accounts.json', 'r', encoding=encoding) as f:
                    content = f.read().strip()
                break
            except UnicodeDecodeError:
                continue
        
        if content is None:
            raise ValueError("Unable to read the file with any of the attempted encodings.")
        
        # Remove BOM if present
        if content.startswith(u'\ufeff'):
            content = content[1:]
        
        # Remove any leading/trailing whitespace and non-printable characters
        content = ''.join(char for char in content if char.isprintable() or char.isspace())
        
        # Parse JSON
        accounts = json.loads(content)
        
        for account in accounts:
            try:
                username = account['username']
                password = account['password']
                proxy = account.get('proxy')
                user_agent = account.get('user_agent', random.choice(MOBILE_USER_AGENTS))
                print(f"\nAttempting to retrieve cookie for {username}")
                print(f"Using proxy: {proxy}")
                print(f"Using user agent: {user_agent}")
                
                max_retries = 3
                for attempt in range(max_retries):
                    cookie = login_to_instagram(username, password, proxy, user_agent)
                    if cookie:
                        print(f"Cookie retrieved successfully for {username}")
                        with open('Files/cookies.txt', 'a', encoding='utf-8') as f:
                            f.write(f"\n{username} - '{cookie}'")
                        print(f"Cookie for {username} saved to 'Files/cookies.txt'")
                        break
                    else:
                        print(f"Failed to retrieve cookie for {username} (Attempt {attempt + 1}/{max_retries})")
                        if attempt < max_retries - 1:
                            delay = (2 ** attempt) + random.random()
                            print(f"Retrying in {delay:.2f} seconds...")
                            time.sleep(delay)
                else:
                    print(f"Failed to retrieve cookie for {username} after {max_retries} attempts")
            except Exception as e:
                print(f"Error processing account {username}: {str(e)}")
                continue
            
            # Add a delay between accounts to avoid rate limiting
            time.sleep(random.uniform(5, 10))
        
        print("\nAll accounts processed.")
    except FileNotFoundError:
        print("Error: 'Files/instagram_accounts.json' not found.")
    except json.JSONDecodeError as j:
        print(f"Error: Invalid JSON in 'Files/instagram_accounts.json'. {j}")
        print("Content of the file:")
        print(content)
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()

def main():
    print("1. Enter credentials manually")
    print("2. Retrieve cookies from JSON file")
    choice = input("Enter your choice (1 or 2): ")
    
    if choice == '1':
        username = input("Enter your Instagram username: ")
        password = input("Enter your Instagram password: ")
        proxy = input("Enter your proxy (leave blank if none): ")
        user_agent = input("Enter your user agent (leave blank to use a default mobile user agent): ")
        
        if not user_agent:
            user_agent = random.choice(MOBILE_USER_AGENTS)
        
        cookie = login_to_instagram(username, password, proxy if proxy else None, user_agent)
        
        if cookie:
            print("\nCookie retrieved successfully:")
            print(cookie)
            
            with open('Files/cookies.txt', 'a') as f:
                f.write(f"\n{username} - '{cookie}'")
            print("\nCookie saved to 'Files/cookies.txt'")
        else:
            print("Failed to retrieve cookie.")
    elif choice == '2':
        retrieve_cookies_from_json()
    else:
        print("Invalid choice. Exiting.")

if __name__ == "__main__":
    main()