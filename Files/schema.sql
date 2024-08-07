CREATE DATABASE IF NOT EXISTS main;
USE main;

CREATE TABLE IF NOT EXISTS users (
    username VARCHAR(255) PRIMARY KEY,
    full_name VARCHAR(255),
    biography TEXT,
    follower_count INT,
    following_count INT,
    media_count INT,
    is_private BOOLEAN,
    is_verified BOOLEAN,
    category VARCHAR(255),
    external_url TEXT,
    public_email VARCHAR(255),
    public_phone_number VARCHAR(255),
    is_business BOOLEAN,
    profile_pic_url TEXT,
    hd_profile_pic_url TEXT,
    has_highlight_reels BOOLEAN,
    has_guides BOOLEAN,
    is_interest_account BOOLEAN,
    total_igtv_videos INT,
    total_clips_count INT,
    total_ar_effects INT,
    is_eligible_for_smb_support_flow BOOLEAN,
    is_eligible_for_lead_center BOOLEAN,
    account_type INT,
    is_call_to_action_enabled BOOLEAN,
    interop_messaging_user_fbid BIGINT,
    has_videos BOOLEAN,
    total_video_count INT,
    has_music_on_profile BOOLEAN,
    is_potential_business BOOLEAN,
    is_memorialized BOOLEAN,
    gender VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS bio_links (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255),
    url TEXT,
    FOREIGN KEY (username) REFERENCES users(username) ON DELETE CASCADE,
    UNIQUE KEY (username, url(255))
);

CREATE TABLE IF NOT EXISTS pinned_channels (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255),
    title VARCHAR(255),
    subtitle TEXT,
    invite_link TEXT,
    number_of_members INT,
    FOREIGN KEY (username) REFERENCES users(username) ON DELETE CASCADE,
    UNIQUE KEY (username, title)
);

CREATE TABLE IF NOT EXISTS followers (
    username VARCHAR(255) PRIMARY KEY,
    source_account VARCHAR(255),
    pk BIGINT,
    pk_id BIGINT,
    full_name VARCHAR(255),
    is_private BOOLEAN,
    fbid_v2 BIGINT,
    third_party_downloads_enabled BOOLEAN,
    strong_id BIGINT,
    profile_pic_id VARCHAR(255),
    profile_pic_url TEXT,
    is_verified BOOLEAN,
    has_anonymous_profile_picture BOOLEAN,
    account_badges JSON,
    latest_reel_media BIGINT,
    is_favorite BOOLEAN,
    gender VARCHAR(20),
    csv_filename VARCHAR(255)
);