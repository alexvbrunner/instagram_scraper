from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import os
import stat
import random

def load_proxies():
    """Load and format proxies with credentials for rotating proxies."""
    username = "qfkdohpl-rotate"
    password = "rdohpw8rodyc"
    domain = "p.webshare.io"
    port = 80
    formatted_proxy = f"http://{username}:{password}@{domain}:{port}"
    print(f"Using proxy: {formatted_proxy}")  # Debug print
    return [formatted_proxy]

def scrape_instagram_head(username, proxies):
    url = f"https://www.instagram.com/{username}/"
    
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    
    if proxies:
        proxy = random.choice(proxies)
        print(f"Selected proxy: {proxy}")  # Debug print
        chrome_options.add_argument(f'--proxy-server={proxy}')
    
    
    # Set up the driver with a specific path
    driver_path = os.path.join(os.path.expanduser("~"), ".wdm", "drivers", "chromedriver", "mac64", "127.0.6533.88", "chromedriver-mac-arm64", "chromedriver")
    
    # Set executable permissions
    os.chmod(driver_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH)
    
    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
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

# Example usage
username = "gringa.ecom"
proxies = load_proxies()
result = scrape_instagram_head(username, proxies)

if result:
    print("\nHead tag found and exported successfully.")
else:
    print("Failed to retrieve or export the head tag.")