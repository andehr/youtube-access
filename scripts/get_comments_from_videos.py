import api
from auth import auth
import os
import pandas as pd
import json
from . import utils

# CONFIG
this_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(this_dir, '..', 'data')

video_jsonl_path = os.path.join(data_dir, 'videos.jsonl')
log_output_path = os.path.join(data_dir, 'comments_from_videos.log')
video_error_path = os.path.join(data_dir, 'video_errors.jsonl')
comments_output_path = os.path.join(data_dir, 'comments.jsonl')

seed_urls = list(pd.read_csv(os.path.join(data_dir, 'seeds.csv'))['url'].values)
seed_users, seed_channels, seed_videos = utils.extract_ids_from_urls(seed_urls)

# logger = utils.build_logger(log_output_path)
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
