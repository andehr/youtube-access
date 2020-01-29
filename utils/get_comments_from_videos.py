import api
from auth import auth
import os
import pandas as pd
import re
import json
import logging

CHANNEL_PATTERN = '.*/channel/([^/&?=]+)'
VIDEO_PATTERN = '.*/watch[?]v=([^/&?=]+)'
USER_PATTERN = '.*/user/([^/&?=]+)'

# CONFIG
this_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(this_dir, '..', 'data')


seed_urls = list(pd.read_csv(os.path.join(data_dir, 'seeds.csv'))['url'].values)

video_jsonl_path = os.path.join(data_dir, 'videos_v2.jsonl')

log_output_path = os.path.join(data_dir, 'comments_from_videos.log')

video_error_path = os.path.join(data_dir, 'video_errors_v2.jsonl')
comments_output_path = os.path.join(data_dir, 'comments_v2.jsonl')


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

    logger.warning('This is a warning')
    logger.error('This is an error')

    return logger


def extract_by_pattern(urls, pattern):
    extractions = []
    for url in urls:
        match = re.match(pattern, url)
        if match:
            extractions.append(match.group(1))
    return extractions


seed_channels = extract_by_pattern(seed_urls, CHANNEL_PATTERN)
seed_users = extract_by_pattern(seed_urls, USER_PATTERN)
seed_videos = extract_by_pattern(seed_urls, VIDEO_PATTERN)

print(f'seed_urls (sample): {seed_urls[:25]}')
print(f'seed channels (sample): {seed_channels[:25]}')
print(f'seed users (sample): {seed_users[:25]}')
print(f'seed videos (sample): {seed_videos[:25]}')

# Ensure none lost/added during extraction
assert sum([len(x) for x in [seed_channels, seed_videos, seed_users]]) == len(seed_urls)

# logger = build_logger(log_output_path)
client = api.YoutubeClient(auth.credentials, rate_limit_retry=30, logger=None)
video_ids = []

with open(video_jsonl_path, 'r') as fp:
    for video_jsonl in fp:
        video_ids.append(json.loads(video_jsonl)['video_id'])

# Add seed video ids
video_ids.extend(seed_videos)

for video_id in video_ids:
    try:
        print('COLLECTING COMMENTS FOR VIDEO:', video_id)
        comment_objs = client.get_comments(video_id, limit=100)
        with open(comments_output_path, 'a') as fp:
            for comment in comment_objs:
                fp.write(f'{json.dumps(comment.print_dict)}\n')

    except Exception as e:
        print('ERROR FOR VIDEO:', video_id)
        with open(video_error_path, 'a') as fp:
            error_json = json.dumps({
                'video_id': video_id,
                'exception': str(e),
            })
            fp.write(f'{error_json}\n')

print('COMPLETE.')
