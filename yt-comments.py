from api import *
import auth
import math
import os
from googleapiclient.errors import HttpError
from pathlib import Path
import argparse


client = YoutubeClient(auth.credentials)

def parse_args():
    '''Set up command line argument parsing'''
    parser = argparse.ArgumentParser()

    parser.add_argument("-u", "--convert-users", type=valid_path_arg,
        help="path to file of user link/id per line")

    parser.add_argument("-c", "--search-channels", type=valid_path_arg,
        help="path to file of channel link/id per line")

    parser.add_argument("-k", "--search-keywords", type=valid_path_arg,
    	help="path to file of keyword per line")

    parser.add_argument("-v", "--search-videos", type=valid_path_arg,
        help="path to file of video ID per line")

    parser.add_argument("--comments-output", type=valid_path_arg, default="comments.csv",
    	help="path to comments output")

    parser.add_argument("--video-output", type=valid_path_arg, default="videos.csv",
        help="path to videos output")

    parser.add_argument("--since", default="01/01/2019",
        help="do not search before this date (DD/MM/YYYY)")

    parser.add_argument("--order", default="viewCount",
        help="order in which to return IDs")

    parser.add_argument("--limit", default=math.inf, type=int,
        help="limit number of pages of videos returned from each search")

    parser.add_argument("--limit-comments", default=math.inf, type=int,
        help="limit the number of pages of comments return from each comment")

    return parser.parse_args()

def valid_path_arg(path):
    return Path(path)

def read_file_args(path):
    with path.open() as argfile:
        for line in argfile:
            yield line.strip()

def id_from_link_or_id(link_or_id):
    if link_or_id.endswith("/"):
        link_or_id = link_or_id[:-1]
    if "/" in link_or_id:
        link_or_id = link_or_id.rsplit("/", 1)[1]
    return link_or_id

def is_channel(link):
    return "/channel" in link

def is_user(link):
    return "/user" in link

def is_link(link):
    return "/" in link

def convert_users_to_channels(args):
    with args.convert_users.with_suffix(".converted.txt").open("w") as output:
        for link_or_id in read_file_args(args.convert_users):
            if not is_link(link_or_id) or is_user(link_or_id):
                user_id = id_from_link_or_id(link_or_id)
                channels = client.get_user_channels(user_id, limit=args.limit)
                for channel in channels:
                    output.write("https://www.youtube.com/channel/" + channel.id + "\n")
            else:
                print(link_or_id + " is not user, copying without converting.")
                output.write(link_or_id + "\n")

def get_videos_from_channels(args):
    for channel in read_file_args(args.search_channels):
        channel = id_from_link_or_id(channel)
        for video in client.search_by_channel(channel, since=args.since, limit=args.limit, order=args.order):
            yield video

def get_videos_from_keywords(args):
    keywords = " ".join(read_file_args(args.search_keywords))
    for video in client.search_by_keyword(keywords, limit=args.limit, order=args.order, since=args.since):
        yield video

def comments_from_videos(videos, args):
    for video in videos:
        try:
            for comment in client.get_comments(video, limit=args.limit_comments):
                yield comment
        except HttpError as e:
            print(e)

if __name__ == "__main__":
    # Get program arguments
    args = parse_args()

    for arg in vars(args):
        print (arg, getattr(args, arg))

    video_ids = []

    if args.search_videos:
        print("> Reading video IDs")
        video_ids += read_file_args(args.search_videos)

    if args.convert_users:
        print("> Converting users to channels")
        convert_users_to_channels(args)

    if args.search_channels:
        print("> Search channels")
        video_ids += get_videos_from_channels(args)

    if args.search_keywords:
        print("> Searching keywords")
        video_ids += get_videos_from_keywords(args)

    if (args.search_channels or args.search_keywords):

        print("> Number of videos found:", len(video_ids))

        print("> Retrieving comments")
        comments = comments_from_videos(video_ids, args)

        print("> Writing comments")
        write_objects_to_csv(list(comments), args.comments_output)

        print("> Retrieving video meta")
        video_queries = assemble_query(video_ids, existing=args.video_output)
        videos = []
        for q in video_queries:
            try:
                videos += client.get_videos(q)
            except HttpError as e:
                print(e)

        print("> Writing videos")
        write_objects_to_csv(list(videos), args.video_output)

        print("> Done")





