import requests
import pandas as pd
from datetime import datetime
import random
import logging
import time
import threading
import numpy as np
import os
import json
import concurrent.futures

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CookieState:
    def __init__(self, cookie, index):
        self.cookie = cookie
        self.index = index
        self.active = True
        self.last_request_time = 0
        self.fail_count = 0

class InstagramScraper:
    def __init__(self, user_id, cookies):
        self.user_id = user_id
        self.cookie_states = [CookieState(cookie, i) for i, cookie in enumerate(cookies)]
        self.base_url = f"https://i.instagram.com/api/v1/friendships/{user_id}/followers/"
        self.params = {'count': 25}
        self.followers = []
        self.cookie_state_lock = threading.Lock()
        self.max_id_lock = threading.Lock()
        self.min_request_interval = 1
        self.total_followers_scraped = 0
        self.base_encoded_part = None
        self.large_step = 250
        self.small_step = 25
        self.global_iteration = 0
        self.last_max_id = "0|"
        self.current_cookie_index = 0
        self.max_retries = 3
        self.stop_event = threading.Event()
        self.max_workers = len(cookies)

    def load_state(self):
        if os.path.exists('scraper_state.json'):
            with open('scraper_state.json', 'r') as f:
                state = json.load(f)
            self.user_id = state['user_id']
            self.cookie_states = []
            for cs in state['cookie_states']:
                cookie_state = CookieState(cs['cookie'], cs['index'])
                cookie_state.active = cs['active']
                cookie_state.last_request_time = cs['last_request_time']
                cookie_state.fail_count = cs.get('fail_count', 0)
                self.cookie_states.append(cookie_state)
            self.total_followers_scraped = state['followers_count']
            self.base_encoded_part = state.get('base_encoded_part')
            self.global_iteration = state.get('global_iteration', 0)
            self.last_max_id = state.get('last_max_id', "0|")
            self.current_cookie_index = state.get('current_cookie_index', 0)
            print(f"Resumed scraping. Previously scraped {self.total_followers_scraped} followers.")
            print(f"Global iteration: {self.global_iteration}, Last max_id: {self.last_max_id}")
        else:
            print("No previous state found. Starting fresh scrape.")
            self.total_followers_scraped = 0
            self.global_iteration = 0
            self.last_max_id = "0|"
            self.current_cookie_index = -1

    def get_base_encoded_part(self):
        for cookie_state in self.cookie_states:
            try:
                followers = self.fetch_followers(cookie_state, initial_request=True)
                if followers and 'next_max_id' in followers:
                    self.base_encoded_part = followers['next_max_id'].split('|')[1]
                    return
            except Exception as e:
                print(f"Error fetching initial followers: {e}")
        raise Exception("Failed to get base encoded part from any cookie")

    def scrape_followers(self):
        self.load_state()
        if not self.base_encoded_part:
            self.get_base_encoded_part()

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.scrape_with_cookie, cookie_state) 
                       for cookie_state in self.cookie_states]
            concurrent.futures.wait(futures)

        print("Scraping complete.")
        self.save_state()

    def scrape_with_cookie(self, cookie_state):
        while not self.stop_event.is_set():
            next_max_id = self.get_next_max_id()
            if next_max_id is None:
                break

            print(f"Fetching with cookie {cookie_state.index + 1}, max_id: {next_max_id}")
            
            params = self.params.copy()
            params['max_id'] = next_max_id
            followers = self.fetch_followers(cookie_state, params)
            
            if followers:
                with self.cookie_state_lock:
                    self.followers.extend(followers['users'])
                    self.total_followers_scraped += len(followers['users'])
                    print(f"Total followers scraped: {self.total_followers_scraped}")
                    self.save_followers(followers['users'])
                    if self.global_iteration >= 3:
                        self.global_iteration += 1  # Increment for small steps
                cookie_state.fail_count = 0  # Reset fail count on success
            else:
                print(f"No followers returned for cookie {cookie_state.index + 1}")
                cookie_state.fail_count += 1
                if cookie_state.fail_count >= 3:
                    cookie_state.active = False
                    print(f"Deactivating cookie {cookie_state.index + 1} due to repeated failures.")
                    break

            self.save_state()

    def fetch_followers(self, cookie_state, params=None, initial_request=False):
        if params is None:
            params = self.params.copy()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 12_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Instagram 105.0.0.11.118 (iPhone11,8; iOS 12_3_1; en_US; en-US; scale=2.00; 828x1792; 165586599)',
            'Cookie': cookie_state.cookie
        }

        self.wait_with_jitter()
        for _ in range(self.max_retries):
            try:
                response = requests.get(self.base_url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()
                
                cookie_state.last_request_time = time.time()
                
                if 'users' not in data or not data['users']:
                    print(f"No more followers found for cookie {cookie_state.index + 1}")
                    return None

                return data
            except requests.exceptions.HTTPError as e:
                if e.response.status_code in [400, 429]:
                    print(f"Error {e.response.status_code}: Possible rate limit for cookie {cookie_state.index + 1}. Current max_id: {params.get('max_id')}")
                    return None
                else:
                    print(f"Request failed: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")
            
            time.sleep(5)  # Wait before retrying
        
        print(f"Max retries reached for cookie {cookie_state.index + 1}")
        return None

    def wait_with_jitter(self):
        activity_type = random.choices(['quick', 'normal', 'engaged'], weights=[0.3, 0.5, 0.2])[0]
        
        if activity_type == 'quick':
            jitter = np.random.exponential(scale=2)
        elif activity_type == 'normal':
            jitter = np.random.normal(loc=10, scale=5)
        else:  # engaged
            jitter = np.random.normal(loc=30, scale=10)

        # Add micro-breaks
        if random.random() < 0.1:  # 10% chance of a micro-break
            jitter += np.random.uniform(60, 300)  # 1-5 minute break

        # Ensure minimum wait time
        jitter = max(jitter, 1)

        print(f"Waiting for {jitter:.2f} seconds.")
        time.sleep(jitter)

    def save_followers(self, followers):
        if not followers:
            print("No followers to save.")
            return

        output_dir = 'instagram_followers'
        os.makedirs(output_dir, exist_ok=True)

        filename = f"{output_dir}/followers_{self.user_id}.csv"

        columns_to_save = {
            'pk': 'user_id',
            'username': 'username',
            'full_name': 'full_name',
            'is_private': 'is_private',
            'is_verified': 'is_verified',
            'profile_pic_url': 'profile_pic_url'
        }

        df = pd.DataFrame(followers)
        df_to_save = df[columns_to_save.keys()].rename(columns=columns_to_save)

        # Check if the file exists to determine whether to write headers
        file_exists = os.path.isfile(filename)
        
        # Append to the CSV file without writing the index
        df_to_save.to_csv(filename, mode='a', header=not file_exists, index=False)

        print(f"Appended {len(followers)} followers to {filename}")

    def get_next_max_id(self):
        with self.max_id_lock:
            if self.global_iteration < 3:
                next_count = (self.global_iteration + 1) * self.large_step
                self.global_iteration += 1
            else:
                current_count = int(self.last_max_id.split('|')[0])
                next_count = current_count + self.small_step
            
            self.last_max_id = f"{next_count}|{self.base_encoded_part}"
            return self.last_max_id

    def save_state(self):
        state = {
            'user_id': self.user_id,
            'cookie_states': [
                {
                    'cookie': cs.cookie,
                    'index': cs.index,
                    'active': cs.active,
                    'last_request_time': cs.last_request_time,
                    'fail_count': cs.fail_count
                } for cs in self.cookie_states
            ],
            'followers_count': self.total_followers_scraped,
            'base_encoded_part': self.base_encoded_part,
            'global_iteration': self.global_iteration,
            'last_max_id': self.last_max_id,
            'current_cookie_index': self.current_cookie_index
        }
        with open('scraper_state.json', 'w') as f:
            json.dump(state, f)

def main():
    user_id = "25922742395"
    cookies = [
        "mid=ZpZeDQAEAAHuc0IRe4KVoPgzAdLR; ig_did=36D34B39-BE6C-47C3-A307-52C8E408F81E; datr=OF6WZrvsarLn4bMSQpzdcNY7; shbid=\"1280\0547943566320\0541754224106:01f76f470a0474fbe7a0882e925071bcf8452ca7209139ee40bdb7fd08a9ddec40931021\"; shbts=\"1722688106\0547943566320\0541754224106:01f72cb78207bb65e7b518f7e9724f3761d23c065e2500878da8f95ef4077759919a5f03\"; csrftoken=Ya2bWZTCamRniAb3mLnLqX3lQZIooYII; ds_user_id=7943566320; sessionid=7943566320%3AgPttqfmrEoTzP9%3A10%3AAYdWHJ9ja6LaNFGpIdZhxzGGHsbHbUbxvjOfMEtxvQ; rur=\"LDC\0547943566320\0541754475025:01f7a4f34e41af7cd8d4a139ac1802c9368a17c591dd9b4784de6040ac1c686c35d6a61e\"; wd=459x812",
        "id=ZgX87QAEAAHzJsaP1ApQRHDTTYNA; ig_did=CCE9277B-2C27-40A4-98EA-0FAE8B344DBC; datr=Cf0FZqbGHWV-HhICttgz3_md; fbm_124024574287414=base_domain=.instagram.com; ps_n=1; ps_l=1; ig_direct_region_hint=\"FRC\0541544968888\0541754123892:01f782b791cd71095797590a565953533138b401f9583291cbb7db9a94bf86378fae51b5\"; shbid=\"967\0541544968888\0541754399817:01f7be3b753f9c85c4936350334a958798bcb2394cf82730bf898e7d38d5a73b12a5fb6d\"; shbts=\"1722863817\0541544968888\0541754399817:01f70f32c161bb014ecebef5204863dd64640906d11e78edf298447346bed1c2c9ef2309\"; oo=v1; csrftoken=KwXZnUhcfv10igmTSKa8ue3xcuaUCQh6; ds_user_id=68656457205; sessionid=68656457205%3A8wZVoqy4Cqz6Tz%3A21%3AAYcGWBXEHJQk0_Ra4jB1CYdlJT2KJmWPqrJXMCAX0w; fbsr_124024574287414=Du4iKV-EUmPTMwOek7x-htwU6QyREmNp1hDblCsm2zk.eyJ1c2VyX2lkIjoiMTAwMDk0MzQ0OTE4ODY0IiwiY29kZSI6IkFRQU1aOFAzbnNYYklkRGR2UloySzZQSzRwR1M0QlExeUk2b210NXliblFBa3kxSklyN1pfYVM5YXpuRWcxVUZkNjZHdVhLdGlZRjV3NUZ5VGt6dnpFaXVXZE9XcWxhYmRzeWF2bUROX1dzcm9VZk90TFhyQ3I4RDhnT1JHZTFNU1JxZUNNTnA5NXdBY0ZsT0s3Zm5EQjhUUFdjX1BXNTV4cXlaV2swWjNzRk5rSmZRajlPeF9wSFZkckFObTNWNjJLcWwzRms5djFnYThlaXBidG5zU3ppZjZjQU92elhOdFVfSDN1VmNxMkZSeVRHR2JTVVlrUXBBZVN0MnBOZXRfTmNpbmI5M0dlcFk4UlhWZHZtZ2h5OWZvcTBhUUIxZUIxcmN2YnhkTVBteC0wcmtFWWxGVGJaWTdidWd0OWN2b3BDYnUyc0lkazhHdmk3T3ltNEdMaXhRIiwib2F1dGhfdG9rZW4iOiJFQUFCd3pMaXhuallCTzdDeElkNTh6ZFNiN0hRR3dhblpCNG5USm1BejU0QTZ2RTJkWkM3bXlUZUJPUjlGTDI3TkdTSVQ5cmlERXRHeEdOb2hQN3dGN2NoN2JSYVAzOUZkQnJweUZoYkxNVDJ4NjR1bTdKQ3J3RU5ybExOR1U0b21Gd1NOZHVyazJMaWI1ZkxMNWNGMTk2dWFVM2hlTzZrVk1zZEVFZDNjWkFScHI2a3ZOS21NVUZaQVE5TVpBUXdxQWczVDhYTkN2NXFVWkQiLCJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImlzc3VlZF9hdCI6MTcyMjk2MzczOH0; fbsr_124024574287414=Du4iKV-EUmPTMwOek7x-htwU6QyREmNp1hDblCsm2zk.eyJ1c2VyX2lkIjoiMTAwMDk0MzQ0OTE4ODY0IiwiY29kZSI6IkFRQU1aOFAzbnNYYklkRGR2UloySzZQSzRwR1M0QlExeUk2b210NXliblFBa3kxSklyN1pfYVM5YXpuRWcxVUZkNjZHdVhLdGlZRjV3NUZ5VGt6dnpFaXVXZE9XcWxhYmRzeWF2bUROX1dzcm9VZk90TFhyQ3I4RDhnT1JHZTFNU1JxZUNNTnA5NXdBY0ZsT0s3Zm5EQjhUUFdjX1BXNTV4cXlaV2swWjNzRk5rSmZRajlPeF9wSFZkckFObTNWNjJLcWwzRms5djFnYThlaXBidG5zU3ppZjZjQU92elhOdFVfSDN1VmNxMkZSeVRHR2JTVVlrUXBBZVN0MnBOZXRfTmNpbmI5M0dlcFk4UlhWZHZtZ2h5OWZvcTBhUUIxZUIxcmN2YnhkTVBteC0wcmtFWWxGVGJaWTdidWd0OWN2b3BDYnUyc0lkazhHdmk3T3ltNEdMaXhRIiwib2F1dGhfdG9rZW4iOiJFQUFCd3pMaXhuallCTzdDeElkNTh6ZFNiN0hRR3dhblpCNG5USm1BejU0QTZ2RTJkWkM3bXlUZUJPUjlGTDI3TkdTSVQ5cmlERXRHeEdOb2hQN3dGN2NoN2JSYVAzOUZkQnJweUZoYkxNVDJ4NjR1bTdKQ3J3RU5ybExOR1U0b21Gd1NOZHVyazJMaWI1ZkxMNWNGMTk2dWFVM2hlTzZrVk1zZEVFZDNjWkFScHI2a3ZOS21NVUZaQVE5TVpBUXdxQWczVDhYTkN2NXFVWkQiLCJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImlzc3VlZF9hdCI6MTcyMjk2MzczOH0; rur=\"LDC\05468656457205\0541754499758:01f74b9e754d177885e542a99eaccbd0c209dc0c622a68f3149f1a50e09d962c01b91462"
    ]

    scraper = InstagramScraper(user_id, cookies)
    scraper.scrape_followers()

if __name__ == "__main__":
    main()