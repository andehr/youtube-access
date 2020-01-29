import re
import logging

CHANNEL_PATTERN = '.*/channel/([^/&?=]+)'
VIDEO_PATTERN = '.*/watch[?]v=([^/&?=]+)'
USER_PATTERN = '.*/user/([^/&?=]+)'


def build_logger(log_path):
    # Create a custom logger
    logger = logging.getLogger(__name__)

    # Create handlers
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler(log_path)
    c_handler.setLevel(logging.INFO)
    f_handler.setLevel(logging.INFO)

    # Create formatters and add it to handlers
    c_handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
    f_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

    return logger


def extract_by_pattern(urls, pattern):
    extractions = []
    for url in urls:
        match = re.match(pattern, url)
        if match:
            extractions.append(match.group(1))
    return extractions


def extract_ids_from_urls(seed_urls):
    seed_channels = extract_by_pattern(seed_urls, CHANNEL_PATTERN)
    seed_users = extract_by_pattern(seed_urls, USER_PATTERN)
    seed_videos = extract_by_pattern(seed_urls, VIDEO_PATTERN)

    # Ensure none lost/added during extraction
    assert sum(len(x) for x in [seed_channels, seed_videos, seed_users]) == len(seed_urls)

    print(f'seed urls (sample): {seed_urls[:25]}')
    print(f' - seed channels (sample): {seed_channels[:25]}')
    print(f' - seed users (sample): {seed_users[:25]}')
    print(f' - seed videos (sample): {seed_videos[:25]}')

    return seed_channels, seed_users, seed_videos
