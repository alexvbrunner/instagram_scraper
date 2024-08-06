import requests
import json
import gender_guesser.detector as gender
import csv
from itertools import cycle
import time
import datetime
import random
import numpy as np
from UTILS.utils import wait_with_jitter
from UTILS.json_parsing import upload_to_database, parse_user_info

def get_user_info(user_id, cookies_string, proxy):
    # Convert cookies string to dictionary
    cookies = dict(cookie.split('=', 1) for cookie in cookies_string.split('; '))

    url = f"https://i.instagram.com/api/v1/users/{user_id}/info/"
    
    # Updated mobile user agent (Instagram v275.0.0.27.98)
    headers = {
        'User-Agent': 'Instagram 275.0.0.27.98 Android (33/13; 420dpi; 1080x2400; samsung; SM-G991B; o1s; exynos2100; en_US; 458229258)',
        'Accept-Language': 'en-US',
        'Accept-Encoding': 'gzip, deflate',
        'X-IG-Capabilities': '3brTvw==',
        'X-IG-Connection-Type': 'WIFI',
        'X-IG-App-ID': '567067343352427',
    }
    
    # Parse proxy string
    proxy_parts = proxy.split(':')
    proxy_url = f"http://{proxy_parts[2]}:{proxy_parts[3]}@{proxy_parts[0]}:{proxy_parts[1]}"
    
    try:
        response = requests.get(url, cookies=cookies, headers=headers, proxies={'http': proxy_url, 'https': proxy_url}, timeout=10)
        
        # Calculate response size
        response_size = len(response.content)
        print(f"Response size: {response_size} bytes")
        
        # Calculate headers size
        headers_size = len('\r\n'.join(f'{k}: {v}' for k, v in response.headers.items()))
        print(f"Headers size: {headers_size} bytes")
        
        # Calculate total size
        total_size = response_size + headers_size
        print(f"Total size: {total_size} bytes")
        
        print(f"Status Code: {response.status_code}")
        print(f"Headers: {response.headers}")
        print(f"Full Response Text: {response.text}")
        
        return response.json(), total_size
    except requests.RequestException as e:
        print(f"Error occurred: {e}")
        return None, 0

def guess_gender(name):
    d = gender.Detector()
    first_name = name.split()[0]
    return d.get_gender(first_name)

def load_proxies(file_path):
    with open(file_path, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def load_user_ids(file_path):
    with open(file_path, 'r') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # Skip header
        return [row[2] for row in csv_reader if row]

def main():
    # Replace with actual cookies
    cookies = 'mid=ZgX87QAEAAHzJsaP1ApQRHDTTYNA; ig_did=CCE9277B-2C27-40A4-98EA-0FAE8B344DBC; datr=Cf0FZqbGHWV-HhICttgz3_md; fbm_124024574287414=base_domain=.instagram.com; ps_n=1; ps_l=1; ig_direct_region_hint="FRC\0541544968888\0541754123892:01f782b791cd71095797590a565953533138b401f9583291cbb7db9a94bf86378fae51b5"; shbid="967\0541544968888\0541754399817:01f7be3b753f9c85c4936350334a958798bcb2394cf82730bf898e7d38d5a73b12a5fb6d"; shbts="1722863817\0541544968888\0541754399817:01f70f32c161bb014ecebef5204863dd64640906d11e78edf298447346bed1c2c9ef2309"; ds_user_id=63539601661; fbsr_124024574287414=MjagfpvuP8lYekSxmL5M3N80n1pr6zWGW6W16QqdcfE.eyJ1c2VyX2lkIjoiMTAwMDk0MzQ0OTE4ODY0IiwiY29kZSI6IkFRQXlKVHhCbk9LbXpzMlJwLWY5dllBd3M1dzBFQ0sydFpjamVueTVxajZ5UEJjeW5BLUhCSnQ0cUQtbklyOUl5YmtfNzlBUVhNV0huOFJwV0pabDJic2ZpTXJWUVp1SkJEaGxYQnFQZzVxaDlONlYtejRYX1diOXltR3ltb21GalNPeTNuMkxIT0Vucldlc3d1dGwyTjcxMWNIeXhDMEttX2JVSzdPMXVfd2VFRnVyTXRsZHRYYjl5aVliYzVLSG1hUGdIbEFPSnNJQi01d1c5cnB5ZGpRZzB1cWgtNFI1Qjc2R0NCdFpxeWVUOW41ZTNhWTdOS0czRHJOYVRhMXZmR2M5bUI5S01pSTZrSEtNV1RzenZTOTZ1dnlCMzMyRndCZXQ3SjQwRmdXcEJXcXVHLWEtLUV0YmZsNnF6MnExZzJPRjllNnE3NjBvTHJwTGFYTF90M2pTIiwib2F1dGhfdG9rZW4iOiJFQUFCd3pMaXhuallCT3lCWkNwd3NKY3VTQ0lodlVLdU9QTzNrZ0E1M21CWUYycUkzTjhUSWZBV0dEb2tWQWx5cXRpRW9nVzNBcHdoWkFqTlZRWkFRTGM2bHNscmdYZVRKVzNkbDBybzBFeWpBcGJ2WkI4VFZYNzM4Q0RqdzZ0eld6RWhIY1hyWkFyTnU3NElYSGRDVWVTZ1F6QWhjWUl2bjQxREc4MGVsNVlCS1ZSVTNFdE1Dbm1STFlmY2ZzNmMyZTRpeXhoNEt3OUdFWkQiLCJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImlzc3VlZF9hdCI6MTcyMjg3NDg5N30; csrftoken=agLENW66vyB7CshjXmX5ESYzcZalstR2; sessionid=63539601661%3ApfUvj5pZBfwgyB%3A16%3AAYdTtD1UZ5jsQuZxS4Tukk-tvEiGBIP_8NoSvZwPYg; dpr=1.7999999523162842; fbsr_124024574287414=MjagfpvuP8lYekSxmL5M3N80n1pr6zWGW6W16QqdcfE.eyJ1c2VyX2lkIjoiMTAwMDk0MzQ0OTE4ODY0IiwiY29kZSI6IkFRQXlKVHhCbk9LbXpzMlJwLWY5dllBd3M1dzBFQ0sydFpjamVueTVxajZ5UEJjeW5BLUhCSnQ0cUQtbklyOUl5YmtfNzlBUVhNV0huOFJwV0pabDJic2ZpTXJWUVp1SkJEaGxYQnFQZzVxaDlONlYtejRYX1diOXltR3ltb21GalNPeTNuMkxIT0Vucldlc3d1dGwyTjcxMWNIeXhDMEttX2JVSzdPMXVfd2VFRnVyTXRsZHRYYjl5aVliYzVLSG1hUGdIbEFPSnNJQi01d1c5cnB5ZGpRZzB1cWgtNFI1Qjc2R0NCdFpxeWVUOW41ZTNhWTdOS0czRHJOYVRhMXZmR2M5bUI5S01pSTZrSEtNV1RzenZTOTZ1dnlCMzMyRndCZXQ3SjQwRmdXcEJXcXVHLWEtLUV0YmZsNnF6MnExZzJPRjllNnE3NjBvTHJwTGFYTF90M2pTIiwib2F1dGhfdG9rZW4iOiJFQUFCd3pMaXhuallCT3lCWkNwd3NKY3VTQ0lodlVLdU9QTzNrZ0E1M21CWUYycUkzTjhUSWZBV0dEb2tWQWx5cXRpRW9nVzNBcHdoWkFqTlZRWkFRTGM2bHNscmdYZVRKVzNkbDBybzBFeWpBcGJ2WkI4VFZYNzM4Q0RqdzZ0eld6RWhIY1hyWkFyTnU3NElYSGRDVWVTZ1F6QWhjWUl2bjQxREc4MGVsNVlCS1ZSVTNFdE1Dbm1STFlmY2ZzNmMyZTRpeXhoNEt3OUdFWkQiLCJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImlzc3VlZF9hdCI6MTcyMjg3NDg5N30; rur="LDC\05463539601661\0541754410902:01f7e3fa5624d2160871e44bb0b6f75f6be3e46ff8231a57ebf70db8be132ca263d4d2e7"; wd=2850x146'

    proxies = load_proxies('Webshare 10 proxies.txt')
    user_ids = load_user_ids('account_users.csv')

    proxy_cycle = cycle(proxies)

    for user_id in user_ids:
        wait_with_jitter()  # Add cooldown before each request
        
        proxy = next(proxy_cycle)
        print(f"Processing user ID: {user_id} with proxy: {proxy}")
        
        user_info, bandwidth_used = get_user_info(user_id, cookies, proxy)

        if user_info and 'user' in user_info:
            parsed_data = parse_user_info(user_info)
            
            print("\nMain User Information:")
            print(f"Username: {parsed_data['username']}")
            print(f"Full Name: {parsed_data['full_name']}")
            print(f"Follower Count: {parsed_data['follower_count']}")
            print(f"Following Count: {parsed_data['following_count']}")
            print(f"Media Count: {parsed_data['media_count']}")
            print(f"Is Private: {parsed_data['is_private']}")
            print(f"Is Verified: {parsed_data['is_verified']}")
            print(f"Biography: {parsed_data['biography']}")
            
            print(f"\nTotal bandwidth used: {bandwidth_used} bytes")

            # Add gender guessing
            full_name = parsed_data['full_name']
            guessed_gender = guess_gender(full_name)
            print(f"Guessed gender for {full_name}: {guessed_gender}")
            
            # Add guessed gender to parsed_data
            parsed_data['gender'] = guessed_gender
            
            # Upload parsed data to the database
            upload_to_database(parsed_data)
            
            # print("\nFull JSON Response:")
            # print(json.dumps(user_info, indent=2))
        else:
            print(f"Failed to retrieve information for user ID: {user_id}")
        
        print("\n" + "="*50 + "\n")

if __name__ == "__main__":
    main()