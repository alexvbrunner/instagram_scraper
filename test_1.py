import requests
import json

def get_user_info(user_id, cookies_string):
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
    
    response = requests.get(url, cookies=cookies, headers=headers)
    print(f"Status Code: {response.status_code}")
    print(f"Headers: {response.headers}")
    print(f"Full Response Text: {response.text}")
    return response.json()


def main():
    # Replace with actual user ID and cookies
    user_id = "64866843789"
    cookies = 'mid=ZgX87QAEAAHzJsaP1ApQRHDTTYNA; ig_did=CCE9277B-2C27-40A4-98EA-0FAE8B344DBC; datr=Cf0FZqbGHWV-HhICttgz3_md; fbm_124024574287414=base_domain=.instagram.com; ps_n=1; ps_l=1; ig_direct_region_hint="FRC\0541544968888\0541754123892:01f782b791cd71095797590a565953533138b401f9583291cbb7db9a94bf86378fae51b5"; shbid="967\0541544968888\0541754399817:01f7be3b753f9c85c4936350334a958798bcb2394cf82730bf898e7d38d5a73b12a5fb6d"; shbts="1722863817\0541544968888\0541754399817:01f70f32c161bb014ecebef5204863dd64640906d11e78edf298447346bed1c2c9ef2309"; ds_user_id=63539601661; fbsr_124024574287414=MjagfpvuP8lYekSxmL5M3N80n1pr6zWGW6W16QqdcfE.eyJ1c2VyX2lkIjoiMTAwMDk0MzQ0OTE4ODY0IiwiY29kZSI6IkFRQXlKVHhCbk9LbXpzMlJwLWY5dllBd3M1dzBFQ0sydFpjamVueTVxajZ5UEJjeW5BLUhCSnQ0cUQtbklyOUl5YmtfNzlBUVhNV0huOFJwV0pabDJic2ZpTXJWUVp1SkJEaGxYQnFQZzVxaDlONlYtejRYX1diOXltR3ltb21GalNPeTNuMkxIT0Vucldlc3d1dGwyTjcxMWNIeXhDMEttX2JVSzdPMXVfd2VFRnVyTXRsZHRYYjl5aVliYzVLSG1hUGdIbEFPSnNJQi01d1c5cnB5ZGpRZzB1cWgtNFI1Qjc2R0NCdFpxeWVUOW41ZTNhWTdOS0czRHJOYVRhMXZmR2M5bUI5S01pSTZrSEtNV1RzenZTOTZ1dnlCMzMyRndCZXQ3SjQwRmdXcEJXcXVHLWEtLUV0YmZsNnF6MnExZzJPRjllNnE3NjBvTHJwTGFYTF90M2pTIiwib2F1dGhfdG9rZW4iOiJFQUFCd3pMaXhuallCT3lCWkNwd3NKY3VTQ0lodlVLdU9QTzNrZ0E1M21CWUYycUkzTjhUSWZBV0dEb2tWQWx5cXRpRW9nVzNBcHdoWkFqTlZRWkFRTGM2bHNscmdYZVRKVzNkbDBybzBFeWpBcGJ2WkI4VFZYNzM4Q0RqdzZ0eld6RWhIY1hyWkFyTnU3NElYSGRDVWVTZ1F6QWhjWUl2bjQxREc4MGVsNVlCS1ZSVTNFdE1Dbm1STFlmY2ZzNmMyZTRpeXhoNEt3OUdFWkQiLCJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImlzc3VlZF9hdCI6MTcyMjg3NDg5N30; csrftoken=agLENW66vyB7CshjXmX5ESYzcZalstR2; sessionid=63539601661%3ApfUvj5pZBfwgyB%3A16%3AAYdTtD1UZ5jsQuZxS4Tukk-tvEiGBIP_8NoSvZwPYg; dpr=1.7999999523162842; fbsr_124024574287414=MjagfpvuP8lYekSxmL5M3N80n1pr6zWGW6W16QqdcfE.eyJ1c2VyX2lkIjoiMTAwMDk0MzQ0OTE4ODY0IiwiY29kZSI6IkFRQXlKVHhCbk9LbXpzMlJwLWY5dllBd3M1dzBFQ0sydFpjamVueTVxajZ5UEJjeW5BLUhCSnQ0cUQtbklyOUl5YmtfNzlBUVhNV0huOFJwV0pabDJic2ZpTXJWUVp1SkJEaGxYQnFQZzVxaDlONlYtejRYX1diOXltR3ltb21GalNPeTNuMkxIT0Vucldlc3d1dGwyTjcxMWNIeXhDMEttX2JVSzdPMXVfd2VFRnVyTXRsZHRYYjl5aVliYzVLSG1hUGdIbEFPSnNJQi01d1c5cnB5ZGpRZzB1cWgtNFI1Qjc2R0NCdFpxeWVUOW41ZTNhWTdOS0czRHJOYVRhMXZmR2M5bUI5S01pSTZrSEtNV1RzenZTOTZ1dnlCMzMyRndCZXQ3SjQwRmdXcEJXcXVHLWEtLUV0YmZsNnF6MnExZzJPRjllNnE3NjBvTHJwTGFYTF90M2pTIiwib2F1dGhfdG9rZW4iOiJFQUFCd3pMaXhuallCT3lCWkNwd3NKY3VTQ0lodlVLdU9QTzNrZ0E1M21CWUYycUkzTjhUSWZBV0dEb2tWQWx5cXRpRW9nVzNBcHdoWkFqTlZRWkFRTGM2bHNscmdYZVRKVzNkbDBybzBFeWpBcGJ2WkI4VFZYNzM4Q0RqdzZ0eld6RWhIY1hyWkFyTnU3NElYSGRDVWVTZ1F6QWhjWUl2bjQxREc4MGVsNVlCS1ZSVTNFdE1Dbm1STFlmY2ZzNmMyZTRpeXhoNEt3OUdFWkQiLCJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImlzc3VlZF9hdCI6MTcyMjg3NDg5N30; rur="LDC\05463539601661\0541754410902:01f7e3fa5624d2160871e44bb0b6f75f6be3e46ff8231a57ebf70db8be132ca263d4d2e7"; wd=2850x146'

    user_info = get_user_info(user_id, cookies)
    print(json.dumps(user_info, indent=2))

if __name__ == "__main__":
    main()