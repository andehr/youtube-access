# -*- coding: utf-8 -*-

import os
import time
import csv
from auth import auth
import math

import googleapiclient.discovery
from googleapiclient.errors import HttpError


class YoutubeClient:
    """
    This object allows you to interface with the Youtube API.
    Instatiates a 'client' attribute, and contains functions which can be used
    to request data from it.
    """

    def __init__(self, credentials):
        """
        :param credentials: A dictionary of credentials - must include:
            api_key
            client_id
            client_secret
        All of these can be found on your YouTube developer page
        """


        # disable oauthlib's https verification when running locally.
        # *do not* leave this option enabled in production.
        os.environ["oauthlib_insecure_transport"] = "1"

        api_service_name = "youtube"
        api_version = "v3"
        developer_key = credentials['api_key']

        self.client = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=developer_key)

    def search_by_keyword(self, query, limit,
                          since='01/01/2019',
                          per_page='50',
                          order='viewCount'):
        """
        Return video ids related to a search term. Will scroll through pages
        until it has reached limit ids.

        Relevant API docs
        https://developers.google.com/youtube/v3/docs/search/list

        :param client: Youtube client
        :param query: Keywords to search on
        :param limit: Max number of ids to look for
        :param since: Return videos published after this date
        :param per_page: Number of results to request per page (50 YT's limit)
        :param order: Order in which to return IDs. viewCount sorts by views,
        descending
        :return: A list of video ids related to the query
        """
        ids = []

        request = self.client.search().list(
            part='id',
            publishedAfter=self.format_date(since),
            maxResults=per_page,
            q=query,
            order=order
        )
        response = request.execute()
        ids.extend([i['id']['videoId'] for i in response['items']])

        if limit:
            while len(ids) < limit:
                try:
                    next_page_token = response['nextPageToken']
                    try:
                        request = self.client.search().list(
                            part='id',
                            pageToken=next_page_token
                        )
                        response = request.execute()
                        found = [i['id']['videoId'] for i in response['items']]
                        ids.extend(found)
                    except HttpError as e:
                        print("403 on video - likely due to be rate limits.")
                        raise e
                except KeyError:
                    print('Found all videos for search term {}'.format(query))
                    break
        return ids

    def get_videos(self, query, get_stats=True):
        """
        Query the API for details on videos, by id
        :param query: a comma separated list of video ids
        :param get_stats: boolean: Whether to query view count (at extra cost)
        :return: a list of Video objects
        """
        videos = []
        request_part = 'snippet'
        if get_stats:
            request_part += ',statistics'
        request = self.client.videos().list(
            part=request_part,
            id=query
        )
        response = request.execute()
        for item in response['items']:
            videos.append(Video(item))
        return videos


    def format_date(self, date, from_format='%d/%m/%Y'):
        """
        Convert human readable date into RFC3339 for the publishedAt attribute
        """
        to_format = "%Y-%m-%dT00:00:00z"
        published_date = time.strptime(date, from_format)
        return time.strftime(to_format, published_date)


class Video:

    def __init__(self, detail):
        """
        :param detail: Video item returned from API
        """

        self.id = detail['id']
        snippet = detail['snippet']
        self.channel_id = snippet['channelId']
        self.channel_title = snippet['channelTitle']
        self.title = snippet['title']
        self.published_at = snippet['publishedAt']
        self.thumbnail = snippet['thumbnails']['high']['url']
        self.description = snippet['description']
        self.tags = snippet['tags']

        # Init stats attributes in case these are included in detail
        self.dislikes = None
        self.views = None
        self.likes = None
        self.comment_count = None
        self.favourites = None

        # find statistics
        try:
            r_stats = detail['statistics']
            self.dislikes = r_stats['dislikeCount']
            self.views = r_stats['viewCount']
            self.likes = r_stats['likeCount']
            self.comment_count = r_stats['commentCount']
            self.favourites = r_stats['favoriteCount']
        except KeyError:
            # No statistics in record
            pass

        # Define dictionary for printing to CSV
        self.print_headers = [
            'id', 'title', 'description', 'published_at',
            'channel_id',
            'dislikes', 'views', 'likes', 'comment_count', 'favourites',
            'thumbnail'
        ]
        self.print_dict = {}
        for h in self.print_headers:
            self.print_dict[h] = self.__getattribute__(h)


def assemble_query(id_list, length=50):
    """
    In order to save on API limits, YT will allow you to pass 50 ids in the
    'query' parameter. This f assembles those lists into a single
    comma seperated string.

    >>> assemble_query(['a','b','c','d'], length=3)
    ['a,b,c', 'd']

    :param id_list: A list of strings to be queried (e.g. video ids)
    :param length: Max length of lists to be returned
    :return: A list of strings
    """
    final_list = []
    chunks = math.floor(len(id_list) / length) + 1
    for i in range(chunks):
        final_list.append(",".join(id_list[i*length:(i+1)*length]))
    return final_list


if __name__ == "__main__":
    import doctest
    doctest.testmod()
