import requests
import json
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def login_instagram(account_data, proxy=None):
    """
    Logs into Instagram with the given account details and proxy, using only the API.
    """
    # Instagram API URL for user info
    url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={account_data['instagram_username']}"

    headers = {
        'User-Agent': account_data['user_agent'],
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'X-IG-App-ID': '936619743392459',
        'X-ASBD-ID': '198387',
        'X-IG-WWW-Claim': '0',
        'X-Requested-With': 'XMLHttpRequest',
        'Connection': 'keep-alive',
        'Referer': 'https://www.instagram.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
    }

    cookies = {}
    for cookie in account_data['cookies'].split(';'):
        if '=' in cookie:
            name, value = cookie.strip().split('=', 1)
            cookies[name] = value

    proxies = None
    if proxy:
        proxies = {
            'http': proxy,
            'https': proxy
        }

    try:
        response = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
        
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and 'user' in data['data'] and 'id' in data['data']['user']:
                user_id = data['data']['user']['id']
                logger.info(f"Login successful. User ID: {user_id}")
                logger.info(f"Response: {response.text}")
                return user_id
            else:
                logger.warning("User data not found in the response")
        else:
            logger.error(f"Login failed. Status code: {response.status_code}")
            logger.error(f"Response: {response.text}")

    except Exception as e:
        logger.error(f"Error occurred while logging in: {str(e)}")

    return None

# Example usage
account_data = {
    'instagram_username': 'zoeo.gden349la0',
    'instagram_password': '52Tr4Cy0',
    'user_agent': 'Instagram 275.0.0.27.98 Android (33/13; 420dpi; 1080x2400; samsung; SM-G991B; o1s; exynos2100; en_US; 458229258)',
    'cookies': (
        "X-MID=Zo7PDwABAAGT7uLYrnBX1MmBu7iO; "
        "Authorization=Bearer IGT:2:eyJkc191c2VyX2lkIjoiNTkzODAwNjU3NTciLCJzZXNzaW9uaWQiOiI1OTM4MDA2NTc1NyUzQWo0RkVwSVJVQW1BQmtZJTNBMjclM0FBWWRnMHhWZDBkUUhtNnUzcENSTVh5bW9hbW91MGV6RzFrZG90MEhGdHcifQ==; "
        "sessionid=59380065757%3ATrg7qbcsT5FdbH%3A12%3AAYcPWGHiXjLjgUj7y9TirSrIJSCOxJQQV2UodYJcKw; "
        "ds_user_id=59380065757; mid=Y_qVxAABAAGljiiYZ2uY4SLRv4Ck; rur=NAO,59380065757,1714359182:01f7dc25125d30e93b4a5923310eaaa93615fb24d5c3cdda031ec4e1ec85373fb7e16c06"
    ),
    'device_id': '480f561f49b3493b'
}

# Proxy configuration (replace with your proxy details or set to None)
proxy = 'http://dlusijcj:m2wpkk4ib1c1@130.180.234.131:7354'

user_id = login_instagram(account_data, proxy)
if user_id:
    print(f"Successfully logged in. User ID: {user_id}")
else:
    print("Login failed.")