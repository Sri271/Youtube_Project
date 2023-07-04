#YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

import streamlit as st
import pymongo
import sqlite3
from googleapiclient.discovery import build
import pandas as pd

client = pymongo.MongoClient('mongodb://localhost:27017/')
mongodb_db = client['youtube_data']
mongodb_collection = mongodb_db['videos']

sqlite_conn = sqlite3.connect('youtube_data.db')
sqlite_cursor = sqlite_conn.cursor()

youtube_api_key = 'youtube_api_key'
youtube_service = build('youtube', 'v3', developerKey=youtube_api_key)


def get_channel_stats(youtube, channel_ids):
    all_data = []

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=','.join(channel_ids)
    )
    response = request.execute()

    for item in response['items']:
        data = {
            'channelName': item['snippet']['title'],
            'subscribers': item['statistics']['subscriberCount'],
            'views': item['statistics']['viewCount'],
            'totalViews': item['statistics']['videoCount'],
            'playlistId': item['contentDetails']['relatedPlaylists']['uploads']
        }

        all_data.append(data)

    return pd.DataFrame(all_data)


def get_video_ids(youtube, playlist_id):
    video_ids = []

    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()

    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')

    return video_ids


def get_video_details(youtube, video_ids):
    all_video_info = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {
                'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                'statistics': ['viewCount', 'likeCount'],
                'contentDetails': ['duration', 'definition', 'caption']
            }
            video_info = {'video_id': video['id']}

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None

            all_video_info.append(video_info)

    return pd.DataFrame(all_video_info)


def get_comments_in_videos(youtube, video_ids):
    all_comments = []

    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id
            )
            response = request.execute()

            comments_in_video = [comment['snippet']['topLevelComment']['snippet']['textOriginal']
                                 for comment in response['items'][0:10]]
            comments_in_video_info = {'video_id': video_id, 'comments': comments_in_video}

            all_comments.append(comments_in_video_info)

        except:
            print('Could not get comments for video ' + video_id)

    return pd.DataFrame(all_comments)


def store_data_in_mongodb(youtube, channel_ids):
    for channel_id in channel_ids:
        channel_stats_df = get_channel_stats(youtube, [channel_id])
        playlist_ids = channel_stats_df['playlistId'].tolist()
        video_ids = []

        for playlist_id in playlist_ids:
            video_ids.extend(get_video_ids(youtube, playlist_id))

        video_details_df = get_video_details(youtube, video_ids)
        comments_df = get_comments_in_videos(youtube, video_ids)

        data = {
            'channel_id': channel_id,
            'channel_name': channel_stats_df['channelName'][0],
            'subscribers': channel_stats_df['subscribers'][0],
            'video_count': channel_stats_df['totalViews'][0],
            'videos': []
        }

        for _, video in video_details_df.iterrows():
            video_entry = {
                'video_id': video['video_id'],
                'title': video['title'],
                'likes': video['likeCount'],
                'comments': comments_df[comments_df['video_id'] == video['video_id']]['comments'].values.tolist()
            }
            data['videos'].append(video_entry)

        mongodb_collection.insert_one(data)

    st.sidebar.success('Data retrieved and stored in MongoDB successfully!')


def migrate_data_to_sql(selected_channel):
    sqlite_cursor.execute('CREATE TABLE IF NOT EXISTS channels (channel_id TEXT, channel_name TEXT, subscribers INTEGER, video_count INTEGER)')
    sqlite_cursor.execute('CREATE TABLE IF NOT EXISTS videos (video_id TEXT, channel_id TEXT, title TEXT, likes INTEGER)')
    sqlite_cursor.execute('DELETE FROM channels')
    sqlite_cursor.execute('DELETE FROM videos')

    selected_channel_data = mongodb_collection.find_one({'channel_name': selected_channel})
    channel_id = selected_channel_data['channel_id']

    sqlite_cursor.execute('INSERT INTO channels VALUES (?, ?, ?, ?)',
                          (selected_channel_data['channel_id'], selected_channel_data['channel_name'],
                           selected_channel_data['subscribers'], selected_channel_data['video_count']))

    for video in selected_channel_data['videos']:
        sqlite_cursor.execute('INSERT INTO videos VALUES (?, ?, ?, ?)',
                              (video['video_id'], channel_id, video['title'], video['likes']))

    sqlite_conn.commit()
    st.sidebar.success('Data migrated to SQL successfully!')


def search_data_by_channel(channel_query):
    sqlite_cursor.execute('SELECT * FROM channels WHERE channel_name LIKE ?', ('%' + channel_query + '%',))
    search_results = sqlite_cursor.fetchall()
    if search_results:
        df = pd.DataFrame(search_results, columns=['channel_id', 'channel_name', 'subscribers', 'video_count'])
        st.subheader('Search Results:')
        st.dataframe(df)
    else:
        st.sidebar.info('No matching channels found.')


def search_data_by_video(video_query):
    sqlite_cursor.execute('SELECT * FROM videos WHERE title LIKE ?', ('%' + video_query + '%',))
    search_results = sqlite_cursor.fetchall()
    if search_results:
        df = pd.DataFrame(search_results, columns=['video_id', 'channel_id', 'title', 'likes'])
        st.subheader('Search Results:')
        st.dataframe(df)
    else:
        st.sidebar.info('No matching videos found.')

def join_tables():
    sqlite_cursor.execute('SELECT channels.channel_name, videos.title, videos.likes '
                          'FROM channels JOIN videos ON channels.channel_id = videos.channel_id')
    search_results = sqlite_cursor.fetchall()
    if search_results:
        df = pd.DataFrame(search_results, columns=['channel_name', 'video_title', 'likes'])
        st.subheader('Joined Table:')
        st.dataframe(df)
    else:
        st.sidebar.info('No data available for joining tables.')

def display_query_results(results, columns):
    if results:
        df = pd.DataFrame(results, columns=columns)
        st.subheader('Query Results:')
        st.dataframe(df)
    else:
        st.sidebar.info('No data available for the query.')

def query_videos_and_channels():
    sqlite_cursor.execute('SELECT videos.title, channels.channel_name '
                          'FROM videos JOIN channels ON videos.channel_id = channels.channel_id')
    search_results = sqlite_cursor.fetchall()
    display_query_results(search_results, ['Video Title', 'Channel Name'])

def query_channels_with_most_videos():
    sqlite_cursor.execute('SELECT channel_name, COUNT(*) AS video_count '
                          'FROM videos JOIN channels ON videos.channel_id = channels.channel_id '
                          'GROUP BY channel_name '
                          'ORDER BY video_count DESC')
    search_results = sqlite_cursor.fetchall()
    display_query_results(search_results, ['Channel Name', 'Video Count'])

def query_top_10_viewed_videos():
    sqlite_cursor.execute('SELECT videos.title, channels.channel_name '
                          'FROM videos JOIN channels ON videos.channel_id = channels.channel_id '
                          'ORDER BY videos.viewCount DESC '
                          'LIMIT 10')
    search_results = sqlite_cursor.fetchall()
    display_query_results(search_results, ['Video Title', 'Channel Name'])

def query_comments_per_video():
    sqlite_cursor.execute('SELECT videos.title, COUNT(*) AS comment_count '
                          'FROM videos JOIN comments ON videos.video_id = comments.video_id '
                          'GROUP BY videos.video_id, videos.title')
    search_results = sqlite_cursor.fetchall()
    display_query_results(search_results, ['Video Title', 'Comment Count'])

def query_videos_with_highest_likes():
    sqlite_cursor.execute('SELECT videos.title, channels.channel_name '
                          'FROM videos JOIN channels ON videos.channel_id = channels.channel_id '
                          'ORDER BY videos.likes DESC '
                          'LIMIT 10')
    search_results = sqlite_cursor.fetchall()
    display_query_results(search_results, ['Video Title', 'Channel Name'])

def query_likes_and_dislikes_per_video():
    sqlite_cursor.execute('SELECT videos.title, videos.likes ' 'FROM videos')
    search_results = sqlite_cursor.fetchall()
    display_query_results(search_results, ['Video Title', 'Likes'])

def query_total_views_per_channel():
    sqlite_cursor.execute('SELECT channels.channel_name, SUM(videos.viewCount) AS total_views '
                          'FROM channels JOIN videos ON channels.channel_id = videos.channel_id '
                          'GROUP BY channels.channel_id, channels.channel_name')
    search_results = sqlite_cursor.fetchall()
    display_query_results(search_results, ['Channel Name', 'Total Views'])

def query_channels_published_in_2022():
    sqlite_cursor.execute('SELECT DISTINCT channel_name '
                          'FROM channels JOIN videos ON channels.channel_id = videos.channel_id '
                          'WHERE strftime("%Y", videos.publishedAt) = "2022"')
    search_results = sqlite_cursor.fetchall()
    display_query_results(search_results, ['Channel Name'])

def query_average_duration_per_channel():
    sqlite_cursor.execute('SELECT channels.channel_name, AVG(videos.duration) AS average_duration '
                          'FROM channels JOIN videos ON channels.channel_id = videos.channel_id '
                          'GROUP BY channels.channel_id, channels.channel_name')
    search_results = sqlite_cursor.fetchall()
    display_query_results(search_results, ['Channel Name', 'Average Duration'])

def query_videos_with_highest_comments():
    sqlite_cursor.execute('SELECT videos.title, channels.channel_name '
                          'FROM videos JOIN channels ON videos.channel_id = channels.channel_id '
                          'ORDER BY videos.commentCount DESC '
                          'LIMIT 10')
    search_results = sqlite_cursor.fetchall()
    display_query_results(search_results, ['Video Title', 'Channel Name'])
    
def main():
    st.title('YouTube Data Harvesting and Data Warehousing')
    st.sidebar.title('Options')

    option = st.sidebar.selectbox('Select an option', ('Retrieve and Store Data in MongoDB', 'Migrate Data to SQL',
                                                       'Search Data', 'Join Tables'))
    if option == 'Retrieve and Store Data in MongoDB':
        st.sidebar.subheader('Retrieve and Store Data in MongoDB')
        channel_id = st.sidebar.text_input('Enter YouTube Channel ID')
        if st.sidebar.button('Get Channel Details'):
            channel_stats_df = get_channel_stats(youtube_service, [channel_id])
            playlist_ids = channel_stats_df['playlistId'].tolist()
            video_ids = []

            for playlist_id in playlist_ids:
                video_ids.extend(get_video_ids(youtube_service, playlist_id))

            video_details_df = get_video_details(youtube_service, video_ids)
            comments_df = get_comments_in_videos(youtube_service, video_ids)

            # Store data in MongoDB
            data = {
                'channel_id': channel_id,
                'channel_name': channel_stats_df['channelName'][0],
                'subscribers': channel_stats_df['subscribers'][0],
                'video_count': channel_stats_df['totalViews'][0],
                'videos': []
            }

            for _, video in video_details_df.iterrows():
                video_entry = {
                    'video_id': video['video_id'],
                    'title': video['title'],
                    'likes': video['likeCount'],
                    'comments': comments_df[comments_df['video_id'] == video['video_id']]['comments'].tolist()
                }
                data['videos'].append(video_entry)

            mongodb_collection.insert_one(data)
            st.sidebar.success('Data retrieved and stored in MongoDB successfully!')

    elif option == 'Migrate Data to SQL':
        st.sidebar.subheader('Migrate Data to SQL')
        selected_channel = st.sidebar.selectbox('Select a channel to migrate', mongodb_collection.distinct('channel_name'))
        if st.sidebar.button('Migrate Data'):
            migrate_data_to_sql(selected_channel)

    elif option == 'Search Data':
        st.sidebar.subheader('Search Data')
        search_option = st.sidebar.selectbox('Select a search option',
                                             ('Videos and Channels',
                                              'Channels with Most Videos',
                                              'Top 10 Most Viewed Videos',
                                              'Comments per Video',
                                              'Videos with Highest Likes',
                                              'Likes and Dislikes per Video',
                                              'Total Views per Channel',
                                              'Channels Published in 2022',
                                              'Average Duration per Channel',
                                              'Videos with Highest Comments'))

        if search_option == 'Videos and Channels':
            if st.sidebar.button('Search'):
                query_videos_and_channels()
        
        elif search_option == 'Channels with Most Videos':
            if st.sidebar.button('Search'):
                query_channels_with_most_videos()

        elif search_option == 'Top 10 Most Viewed Videos':
            if st.sidebar.button('Search'):
                query_top_10_viewed_videos()

        elif search_option == 'Comments per Video':
            if st.sidebar.button('Search'):
                query_comments_per_video()

        elif search_option == 'Videos with Highest Likes':
            if st.sidebar.button('Search'):
                query_videos_with_highest_likes()

        elif search_option == 'Likes and Dislikes per Video':
            if st.sidebar.button('Search'):
                query_likes_and_dislikes_per_video()

        elif search_option == 'Total Views per Channel':
            if st.sidebar.button('Search'):
                query_total_views_per_channel()

        elif search_option == 'Channels Published in 2022':
            if st.sidebar.button('Search'):
                query_channels_published_in_2022()

        elif search_option == 'Average Duration per Channel':
            if st.sidebar.button('Search'):
                query_average_duration_per_channel()

        elif search_option == 'Videos with Highest Comments':
            if st.sidebar.button('Search'):
                query_videos_with_highest_comments()

    elif option == 'Join Tables':
        st.sidebar.subheader('Join Tables')
        if st.sidebar.button('Join'):
            join_tables()
            

if __name__ == '__main__':
    main()

