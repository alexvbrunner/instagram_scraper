import requests
import json
import time
import schedule
from urllib.parse import urlparse

def check_cookie(proxy, cookie, name):
    proxy_parts = proxy.split(':')
    proxy_url = f"http://{proxy_parts[2]}:{proxy_parts[3]}@{proxy_parts[0]}:{proxy_parts[1]}"
    
    proxies = {
        'http': proxy_url,
        'https': proxy_url
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Cookie': cookie
    }
    
    try:
        response = requests.get('https://www.instagram.com/', headers=headers, proxies=proxies, timeout=10)
        if response.status_code == 200 and 'user_id' in response.text:
            print(f"Cookie for {name} is still valid.")
            return True
        else:
            print(f"Cookie for {name} is no longer valid.")
            return False
    except Exception as e:
        print(f"Error checking cookie for {name}: {str(e)}")
        return False

def check_all_cookies():
    print("\nChecking all cookies...")
    with open('Files/proxy_cookie_pairs.json', 'r') as f:
        pairs = json.load(f)
    
    valid_pairs = []
    for pair in pairs:
        if check_cookie(pair['proxy'], pair['cookie'], pair['name']):
            valid_pairs.append(pair)
    
    with open('Files/proxy_cookie_pairs.json', 'w') as f:
        json.dump(valid_pairs, f, indent=2)
    
    print("Updated proxy_cookie_pairs.json with valid pairs.")

def main():
    schedule.every(30).minutes.do(check_all_cookies)
    
    print("Starting cookie checker. Will check every 30 minutes.")
    check_all_cookies()  # Run immediately on start
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()