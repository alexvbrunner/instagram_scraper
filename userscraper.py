from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import os
import random
import zipfile

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

def scrape_instagram_head(username, proxies):
    url = f"https://www.instagram.com/{username}/"

    # Rotate proxy for each request
    proxy = get_random_proxy(proxies)
    print(f"Using proxy: {proxy}")
    driver = setup_driver(proxy)
    
    try:
        driver.get(url)
        time.sleep(5)  # Wait for the page to load
        
        # Get the page source and parse it with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        meta_tags = soup.find_all('meta')
        
        if meta_tags:
            # Print the meta tags
            print("Meta tags found:")
            for tag in meta_tags:
                print(tag)
            
            # Export the meta tags as HTML
            with open(f"{username}_meta_tags.html", "w", encoding="utf-8") as f:
                f.write("\n".join(str(tag) for tag in meta_tags))
            
            print(f"\nMeta tags exported to {username}_meta_tags.html")
            
            return meta_tags
        else:
            print("No meta tags found")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        driver.quit()

# Load proxies from file
proxies_file_path = "Webshare 10 proxies.txt"
proxies = get_proxies_from_file(proxies_file_path)

# Example usage
username = "gringa.ecom"
result = scrape_instagram_head(username, proxies)

if result:
    print("\nHead tag found and exported successfully.")
else:
    print("Failed to retrieve or export the head tag.")