import api
from auth import auth
import os
import pandas as pd
import json
from . import utils

# CONFIG
this_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(this_dir, '..', 'data')

channel_jsonl_path = os.path.join(data_dir, 'channels.jsonl')
log_output_path = os.path.join(data_dir, 'video_from_channel.log')
channel_error_path = os.path.join(data_dir, 'channel_errors.jsonl')
video_output_path = os.path.join(data_dir, 'videos.jsonl')

seed_urls = list(pd.read_csv(os.path.join(data_dir, 'seeds.csv'))['url'].values)
seed_users, seed_channels, seed_videos = utils.extract_ids_from_urls(seed_urls)

# logger = utils.build_logger(log_output_path)
client = api.YoutubeClient(auth.credentials, rate_limit_retry=30, logger=None)
channel_ids = []

with open(channel_jsonl_path, 'r') as fp:
    for channel_json_line in fp:
        channel_ids.append(json.loads(channel_json_line)['channel_id'])

video_ids = []

# Add seed video ids
with open(video_output_path, 'a') as fp:
    for video_id in seed_videos:
        video_dict = {
            'video_id': video_id,
        }
        fp.write(f'{json.dumps(video_dict)}\n')

# Resolve channels to video ids
for channel_id in channel_ids:
    try:
        print('REQUESTING VIDEO IDS FOR CHANNEL:', channel_id)
        channel_video_ids = client.search_by_channel(channel_id, limit=100)
        video_ids.extend(channel_video_ids)

        with open(video_output_path, 'a') as fp:
            for video_id in channel_video_ids:
                video_dict = {
                    'video_id': video_id,
                    'channel_id': channel_id,
                }
                fp.write(f'{json.dumps(video_dict)}\n')

    except Exception as e:
        print('ERROR WITH CHANNEL:', channel_id)
        with open(channel_error_path, 'a') as fp:
            error_json = json.dumps({
                'video_id': video_id,
                'channel_id': channel_id,
                'exception': str(e),
            })
            fp.write(f'{error_json}\n')

print('COMPLETE.')
