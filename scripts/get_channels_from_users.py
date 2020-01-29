import api
from auth import auth
import os
import pandas as pd
import json
from . import utils

# CONFIG
this_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(this_dir, '..', 'data')

log_output_path = os.path.join(data_dir, 'channels_from_user.log')
user_error_path = os.path.join(data_dir, 'user_errors.jsonl')
channel_output_path = os.path.join(data_dir, 'channels.jsonl')

seed_urls = list(pd.read_csv(os.path.join(data_dir, 'seeds.csv'))['url'].values)
seed_users, seed_channels, seed_videos = utils.extract_ids_from_urls(seed_urls)

# logger = utils.build_logger(log_output_path)
client = api.YoutubeClient(auth.credentials, rate_limit_retry=30, logger=None)
channel_ids = []

# Output seed channels
for channel_id in seed_channels:
    with open(channel_output_path, 'a') as fp:
        channel_dict = {
            'channel_id': channel_id,
        }
        fp.write(f'{json.dumps(channel_dict)}\n')

# Resolve users to channels, and add ids
for user_id in seed_users:
    try:
        print('GETTING CHANNEL IDS FOR USER:', user_id)
        for channel_obj in client.get_user_channels(user_id):
            channel_ids.append(channel_obj.id)
            with open(channel_output_path, 'a') as fp:
                channel_dict = {
                    'channel_id': channel_obj.id,
                    'user_id': user_id,
                }
                fp.write(f'{json.dumps(channel_dict)}\n')

    except Exception as e:
        print('FAILED FOR USER', user_id)
        with open(user_error_path, 'a') as fp:
            error_json = json.dumps({
                'user_id': user_id,
                'exception': str(e),
            })
            fp.write(f'{error_json}\n')

print('COMPLETE.')
