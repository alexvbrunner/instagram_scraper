from mysql.connector import Error
import mysql.connector

def parse_user_info(json_data):
    user = json_data['user']
    parsed_data = {
        'username': user.get('username'),
        'full_name': user.get('full_name'),
        'biography': user.get('biography'),
        'follower_count': user.get('follower_count'),
        'following_count': user.get('following_count'),
        'media_count': user.get('media_count'),
        'is_private': user.get('is_private'),
        'is_verified': user.get('is_verified'),
        'category': user.get('category'),
        'external_url': user.get('external_url'),
        'public_email': user.get('public_email'),
        'public_phone_number': user.get('public_phone_number'),
        'is_business': user.get('is_business'),
        'profile_pic_url': user.get('profile_pic_url'),
        'hd_profile_pic_url': user.get('hd_profile_pic_url_info', {}).get('url'),
        'has_highlight_reels': user.get('has_highlight_reels'),
        'has_guides': user.get('has_guides'),
        'is_interest_account': user.get('is_interest_account'),
        'total_igtv_videos': user.get('total_igtv_videos'),
        'total_clips_count': user.get('total_clips_count'),
        'total_ar_effects': user.get('total_ar_effects'),
        'is_eligible_for_smb_support_flow': user.get('is_eligible_for_smb_support_flow'),
        'is_eligible_for_lead_center': user.get('is_eligible_for_lead_center'),
        'account_type': user.get('account_type'),
        'is_call_to_action_enabled': user.get('is_call_to_action_enabled'),
        'interop_messaging_user_fbid': user.get('interop_messaging_user_fbid'),
        'bio_links': [link.get('url') for link in user.get('bio_links', [])],
        'has_videos': user.get('has_videos'),
        'total_video_count': user.get('total_video_count'),
        'has_music_on_profile': user.get('has_music_on_profile'),
        'is_potential_business': user.get('is_potential_business'),
        'is_memorialized': user.get('is_memorialized'),
        'pinned_channels_info': {
            'has_public_channels': user.get('pinned_channels_info', {}).get('has_public_channels'),
            'channels': [
                {
                    'title': channel.get('title'),
                    'subtitle': channel.get('subtitle'),
                    'invite_link': channel.get('invite_link'),
                    'number_of_members': channel.get('number_of_members')
                }
                for channel in user.get('pinned_channels_info', {}).get('pinned_channels_list', [])
            ]
        },
        'gender': None  # This will be filled in by the main script
    }
    return parsed_data

def upload_to_database(parsed_data):
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            database='main',
            user='root',
            password='password'
        )

        if connection.is_connected():
            print("Successfully connected to the database")
            cursor = connection.cursor()

            # Insert data into the users table
            print(f"Inserting data for user: {parsed_data['username']}")
            insert_query = """
            INSERT INTO users (
                username, full_name, biography, follower_count, following_count,
                media_count, is_private, is_verified, category, external_url,
                public_email, public_phone_number, is_business, profile_pic_url,
                hd_profile_pic_url, has_highlight_reels, has_guides,
                is_interest_account, total_igtv_videos, total_clips_count,
                total_ar_effects, is_eligible_for_smb_support_flow,
                is_eligible_for_lead_center, account_type, is_call_to_action_enabled,
                interop_messaging_user_fbid, has_videos, total_video_count,
                has_music_on_profile, is_potential_business, is_memorialized, gender
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                full_name = VALUES(full_name),
                biography = VALUES(biography),
                follower_count = VALUES(follower_count),
                following_count = VALUES(following_count),
                media_count = VALUES(media_count),
                is_private = VALUES(is_private),
                is_verified = VALUES(is_verified),
                category = VALUES(category),
                external_url = VALUES(external_url),
                public_email = VALUES(public_email),
                public_phone_number = VALUES(public_phone_number),
                is_business = VALUES(is_business),
                profile_pic_url = VALUES(profile_pic_url),
                hd_profile_pic_url = VALUES(hd_profile_pic_url),
                has_highlight_reels = VALUES(has_highlight_reels),
                has_guides = VALUES(has_guides),
                is_interest_account = VALUES(is_interest_account),
                total_igtv_videos = VALUES(total_igtv_videos),
                total_clips_count = VALUES(total_clips_count),
                total_ar_effects = VALUES(total_ar_effects),
                is_eligible_for_smb_support_flow = VALUES(is_eligible_for_smb_support_flow),
                is_eligible_for_lead_center = VALUES(is_eligible_for_lead_center),
                account_type = VALUES(account_type),
                is_call_to_action_enabled = VALUES(is_call_to_action_enabled),
                interop_messaging_user_fbid = VALUES(interop_messaging_user_fbid),
                has_videos = VALUES(has_videos),
                total_video_count = VALUES(total_video_count),
                has_music_on_profile = VALUES(has_music_on_profile),
                is_potential_business = VALUES(is_potential_business),
                is_memorialized = VALUES(is_memorialized),
                gender = VALUES(gender)
            """
            
            user_data = tuple(parsed_data[key] if key in parsed_data else None for key in [
                'username', 'full_name', 'biography', 'follower_count', 'following_count',
                'media_count', 'is_private', 'is_verified', 'category', 'external_url',
                'public_email', 'public_phone_number', 'is_business', 'profile_pic_url',
                'hd_profile_pic_url', 'has_highlight_reels', 'has_guides',
                'is_interest_account', 'total_igtv_videos', 'total_clips_count',
                'total_ar_effects', 'is_eligible_for_smb_support_flow',
                'is_eligible_for_lead_center', 'account_type', 'is_call_to_action_enabled',
                'interop_messaging_user_fbid', 'has_videos', 'total_video_count',
                'has_music_on_profile', 'is_potential_business', 'is_memorialized', 'gender'
            ])

            cursor.execute(insert_query, user_data)
            print(f"Inserted user data for {parsed_data['username']}")

            # Insert data into the bio_links table
            print(f"Inserting {len(parsed_data['bio_links'])} bio links for {parsed_data['username']}")
            for link in parsed_data['bio_links']:
                insert_link_query = """
                INSERT INTO bio_links (username, url)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE url = VALUES(url)
                """
                cursor.execute(insert_link_query, (parsed_data['username'], link))
            print("Bio links inserted successfully")

            # Insert data into the pinned_channels table
            print(f"Inserting {len(parsed_data['pinned_channels_info']['channels'])} pinned channels for {parsed_data['username']}")
            for channel in parsed_data['pinned_channels_info']['channels']:
                insert_channel_query = """
                INSERT INTO pinned_channels (username, title, subtitle, invite_link, number_of_members)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    subtitle = VALUES(subtitle),
                    invite_link = VALUES(invite_link),
                    number_of_members = VALUES(number_of_members)
                """
                channel_data = (
                    parsed_data['username'], channel['title'], channel['subtitle'],
                    channel['invite_link'], channel['number_of_members']
                )
                cursor.execute(insert_channel_query, channel_data)
            print("Pinned channels inserted successfully")

            connection.commit()
            print("All data committed successfully")

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")