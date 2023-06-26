import streamlit as st
import pymongo as mong
from googleapiclient.discovery import build
import googleapiclient.errors
import pandas as pd
import mysql.connector

# MongoDB Atlas connection credentials
atlas_username = 'shilpagupta1604'
atlas_password = 'kUuCrHpXWfmVuciS'
atlas_cluster = 'cluster0'

# Connecting to MongoDB Atlas Client
client = mong.MongoClient(
    f"mongodb+srv://{atlas_username}:{atlas_password}@{atlas_cluster}.uca4ga4.mongodb.net/?retryWrites=true&w=majority")

# Create Mongo connection to database youtube_data with collection as Channel,Playlist and Video
db = client['youtube_data']
channel_collection = db['Channel']
playlist_collection = db['Playlist']
video_collection = db['Video']
comment_collection = db['Comment']
youtube_data_collection = db['YouTube_Data']

# YouTube API connection details

scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

api_service_name = "youtube"
api_version = "v3"
api_key = 'AIzaSyD4jRmL_nt6poMbCSdfLkYGUEi1U6e6Fhw'
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)


#Function to check if key is present in the google data, If the corresponding key(data) id not present, 0 is returned.
def check_value(x, y):
    if x in list(y.keys()):
        return y['items'][0]['statistics'][x]
    else:
        return 0


# Create a connection to MySQL database
def connect_mysql():
    mysql_host = 'localhost'
    mysql_database = 'Youtube'
    mysql_user = 'root'
    mysql_password = 'shilpa'
    mysql_connection = mysql.connector.connect(host=mysql_host,
                                               database=mysql_database,
                                               user=mysql_user,
                                               password=mysql_password)
    return mysql_connection


# Function to interact with youtube API

def youtube_api():
    api_service_name = "youtube"
    api_version = "v3"
    api_key = 'AIzaSyD4jRmL_nt6poMbCSdfLkYGUEi1U6e6Fhw'
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    return youtube


# Function to retrieve Channel Data taking youtube API and Channel ID as input
def channel_data(youtube, channel_id):
    request_channel = youtube.channels().list(part="snippet,statistics", id=channel_id)
    channel_response = request_channel.execute()
    return channel_response


# Function to retrieve Playlist of a YouTube Channel
def playlist_data(youtube, channel_id):
    request_playlist = youtube.playlists().list(part="snippet,contentDetails", channelId=channel_id)
    playlist_response = request_playlist.execute()
    return playlist_response


# Function to retrieve Youtube Video Ids for a Playlist
def playlist_item_data(youtube, playlist_id):
    request = youtube.playlistItems().list(part="snippet,contentDetails", playlistId=playlist_id)
    response = request.execute()
    return response


# Function to retrieve Youtube Video Data using Video ID
def video_data(youtube, video_id):
    request = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_id['Video ID'])
    response = request.execute()
    response['playlistid'] = video_id['Playlist ID']
    return response

# Function to retrieve Youtube Comment Data using Video ID
def comment_data(youtube, video_id):
    request = youtube.commentThreads().list(part="snippet", videoId=video_id)
    response = request.execute()
    return response

# From the channel data returned from google API, relevant channel data is retrieved using this function
def retrieve_relevant_channel_data(channel_list):
    df_list_channel = []

    for item in channel_list:
        df_list_channel.append({'Channel ID': item['items'][0]['id'],
                                'Channel Name ': item['items'][0]['snippet']['title'],
                                'Channel Type': 'ABX',
                                'Channel Views': item['items'][0]['statistics']['viewCount'],
                                'Channel Description': item['items'][0]['snippet']['description'],
                                'Channel Status': 'Enabled'})

    return df_list_channel

# From the playlist data returned from google API, relevant playlist data is retrieved using this function
def retrieve_relevant_playlist_data(playlist_list):
    df_list_playlist = []
    for item in playlist_list:
        for i in range(len(item['items'])):
            df_list_playlist.append({'Playlist ID': item['items'][i]['id'],
                                     'Channel ID': item['items'][i]['snippet']['channelId'],
                                     'Playlist Name': item['items'][i]['snippet']['title']})
    return df_list_playlist

# Covert dates to datetime format
def convert_date(value):
    date_value = (value.replace('T', ' ')).replace('Z', '')
    return pd.to_datetime(date_value, format='%Y-%m-%d %H:%M:%S')

# Function to convert duration value returned from google API into seconds
def parse_duration(duration):
    a = duration[2:]

    if 'H' in a and 'M' in a and 'S' in a:
        hours = int(a[:a.index('H')])
        minute = int(a[a.index('H') + 1:a.index('M')])
        seconds = int(a[a.index('M') + 1:a.index('S')])

    elif 'H' in a and 'M' not in a and 'S' not in a:
        hours = int(a[:a.index('H')])
        minutes = 0
        seconds = 0

    elif 'H' in a and 'M' in a and 'S' not in a:
        hours = int(a[:a.index('H')])
        minute = int(a[a.index('H') + 1:a.index('M')])
        seconds = 0

    elif 'H' in a and 'M' not in a and 'S' in a:
        hours = int(a[:a.index('H')])
        minute = 0
        seconds = int(a[a.index('H') + 1:a.index('S')])

    elif 'H' not in a and 'M' in a and 'S' not in a:
        hours = 0
        minute = int(a[:a.index('M')])
        seconds = 0
    elif 'H' not in a and 'M' in a and 'S' in a:
        hours = 0
        minute = int(a[:a.index('M')])
        seconds = int(a[a.index('M') + 1:a.index('S')])

    elif 'H' not in a and 'M' not in a and 'S' in a:
        hours = 0
        minute = 0
        seconds = int(a[:a.index('S')])

    duration_seconds = (hours * 3600) + (60 * minute) + seconds
    return duration_seconds

# From the video data returned from google API, relevant video data is retrieved using this function
def retrieve_relevant_video_data(video_list):
    df_list_video = []
    for item in video_list:
        df_list_video.append({'Video ID': item['items'][0]['id'],
                              'Playlist ID': item['playlistid'],
                              'Video Name': item['items'][0]['snippet']['title'],
                              'Video Description': item['items'][0]['snippet']['description'],
                              'Published Date': convert_date(item['items'][0]['snippet']['publishedAt']),
                              'View Count': item['items'][0]['statistics']['viewCount'],
                              'Like Count': item['items'][0]['statistics']['likeCount'],
                              'Dislike Count': check_value('dislikeCount', item),
                              'Favorite Count': item['items'][0]['statistics']['favoriteCount'],
                              'Comment Count': item['items'][0]['statistics']['commentCount'],
                              'Duration in seconds': parse_duration(item['items'][0]['contentDetails']['duration']),
                              'Thumbnail': item['items'][0]['snippet']['thumbnails']['default']['url'],
                              'Caption Status': item['items'][0]['contentDetails']['caption']
                              })
    return df_list_video

# From the comment data returned from google API, relevant comment data is retrieved using this function
def retrieve_relevant_comment_data(comment_list):
    df_list_comment = []
    for x in comment_list:
        for item in x['items']:
            df_list_comment.append({'Comment ID': item['id'],
                                    'Video ID': item['snippet']['videoId'],
                                    'Comment Text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                    'Comment Author': item['snippet']['topLevelComment']['snippet'][
                                        'authorDisplayName'],
                                    'Comment_Published Date': convert_date(
                                        item['snippet']['topLevelComment']['snippet']['publishedAt'])})

    return df_list_comment


# Function to insert channel data into MySQL
def insert_channel_into_mysql(connection, cursor, df_list_channel):
    cursor.execute('''select channel_id from Youtube.Channel''')
    channels = cursor.fetchall()
    existing_channels = [item[0] for item in channels]

    for record in df_list_channel: #Checking if the channel id is not already present in the table, then only insert it
        print(record['Channel ID'])
        if record['Channel ID'] not in existing_channels:
            cursor.execute(
                f'''INSERT INTO Youtube.Channel (channel_id, channel_name,channel_type,channel_views,
                channel_description,channel_status) VALUES {tuple(record.values())}''')
    connection.commit()


# Function to insert playlist data into MySQL
def insert_playlist_into_mysql(connection, cursor, df_list_playlist):
    cursor.execute('''select playlist_id from Youtube.Playlist''')
    playlists = cursor.fetchall()
    existing_playlist = [item[0] for item in playlists]

    for record in df_list_playlist: #Checking if the playlist id is not already present in the table, then only insert it
        if record['Playlist ID'] not in existing_playlist:
            cursor.execute(
                f'''INSERT INTO Youtube.Playlist (playlist_id,channel_id, playlist_name) VALUES {tuple(record.values())}''')
    connection.commit()



# Function to insert Video data into MySQL
def insert_video_into_mysql(connection, cursor, df_list_video):
    cursor.execute('''select video_id from Youtube.Video''')
    videos = cursor.fetchall()
    existing_videos = [item[0] for item in videos]

    for record in df_list_video: #Checking if the video id is not already present in the table, then only insert it
        if record['Video ID'] not in existing_videos:
            cursor.execute(
                f'''INSERT INTO Youtube.Video(video_id,playlist_id,video_name,video_description,published_date,
                view_count,like_count,dislike_count,favorite_count,comment_count,duration,thumbnail,caption_status) 
                VALUES {tuple(record.values())}''')
    connection.commit()



# Function to insert Comment data into MySQL
def insert_comment_into_mysql(connection, cursor, df_list_comment):
    cursor.execute('''select comment_id from Youtube.comment''')
    comments = cursor.fetchall()
    existing_comments = [item[0] for item in comments]

    for record in df_list_comment: #Checking if the comment id is not already present in the table, then only insert it
        if record['Comment ID'] not in existing_comments:
            cursor.execute(
                f'''INSERT INTO Youtube.comment(comment_id,video_id,comment_text,comment_author,
                comment_published_date) VALUES {tuple(record.values())}''')
    connection.commit()


#Function to create an emedded dictionary of channel:playlist:video:comment from each individual list
def combine_data(df_list_channel_copy, df_list_playlist_copy, df_list_video_copy, df_list_comment_copy):
    final = []
    for channel_item in df_list_channel_copy:
        channel_item['Playlist'] = []
        for playlist_item in df_list_playlist_copy:
            playlist_item['Video'] = []
            for video_item in df_list_video_copy:
                video_item['Comment'] = []
                for comment_item in df_list_comment_copy:
                    if comment_item['Video ID'] == video_item['Video ID']:
                        video_item['Comment'].append(comment_item)
                if video_item['Playlist ID'] == playlist_item['Playlist ID']:
                    playlist_item['Video'].append(video_item)
            if item['Channel ID'] == playlist_item['Channel ID']:
                channel_item['Playlist'].append(playlist_item)
        final.append(channel_item)
    return final



# Main body of streamlit

st.title(':red[**_Youtube data Harvesting_**]')
st.header('This is Youtube channel data')

st.divider()

#Take channel id input from user
input = st.text_input('Provide YouTube Channel ID', key='channel_id',
                      help='you can enter maximum 10 channel ids. Use comma separated list for multiple ids. For eg: abc,xyz,pqr',
                      placeholder='Channel ID', disabled=False, label_visibility="visible")

channel_input = list(input.split(','))

# If more than 10 Channel Ids are inputted, the below message will be displayed
if len(channel_input) > 10:
    st.write("Only 10 channel inputs allowed. You have exceeded the limit.")

# If any channel id is given in input, corresponding data is retrieved using youtube google api, and lists are created for channel,playlist,video and comment

if len(input) != 0 and len(channel_input) < 10:

    channel_list = [channel_data(youtube, item.strip()) for item in channel_input]
    df_list_channel = retrieve_relevant_channel_data(channel_list)
    # st.write(df_list_channel)
    playlist_list = [playlist_data(youtube, item) for item in channel_input]
    df_list_playlist = retrieve_relevant_playlist_data(playlist_list)

    video_id_list = []

    for item in df_list_playlist:

        playlist_item_response = playlist_item_data(youtube, item['Playlist ID'])
        for playlist_item in playlist_item_response['items']:
            video_id_list.append({'Playlist ID': playlist_item['snippet']['playlistId'],
                                  'Video ID': playlist_item['contentDetails']['videoId']})

    video_list = [video_data(youtube, item) for item in video_id_list]

    df_list_video = retrieve_relevant_video_data(video_list)

    comment_list = [comment_data(youtube, item['Video ID']) for item in video_id_list]
    df_list_comment = retrieve_relevant_comment_data(comment_list)

# By clicking on Retrieve Channel Data button, each list converts to dataframe, and displays on screen a combined dataframe
if st.button('Retrieve Channel Data'):

# Creating dataframes from list
    df_channel = pd.DataFrame(df_list_channel)

    df_playlist = pd.DataFrame(df_list_playlist)
    df_video = pd.DataFrame(df_list_video)
    df_comment = pd.DataFrame(df_list_comment)

# Mergining individual dataframes
    df_video_comment = df_video.merge(df_comment, how='left', on='Video ID')
    df_playlist_video = df_playlist.merge(df_video_comment, how='left', on='Playlist ID')
    df = df_channel.merge(df_playlist_video, how='left', on='Channel ID')

# Display combined dataframe on clicking on retrieve button
    st.write(df)

    final = combine_data(df_list_channel, df_list_playlist, df_list_video, df_list_comment)


# Move a list of embedded document into MongoDB Atlas database
if st.button('Move to MongoDB Atlas'):

    final = combine_data(df_list_channel, df_list_playlist, df_list_video, df_list_comment)
    youtube_data_collection.insert_many(final)
    st.success('Data Stored Successfully in MongoDB YouTube data Database.')

# Move data to MY SQL database.
if st.button('Move data to My SQL'):
    mysql_connection = connect_mysql()

    if mysql_connection.is_connected():
        st.success("Connected to MySQL Server version ")
        cursor = mysql_connection.cursor()

        cursor.execute("SET sql_mode = '';")

        insert_channel_into_mysql(mysql_connection, cursor, df_list_channel)
        insert_playlist_into_mysql(mysql_connection, cursor, df_list_playlist)
        insert_video_into_mysql(mysql_connection, cursor, df_list_video)
        insert_comment_into_mysql(mysql_connection, cursor, df_list_comment)

# Query functions to be executed depending on the input select in the multiselect dropdown

def query1():
    mysql_connection = connect_mysql()
    cursor = mysql_connection.cursor()
    cursor.execute('''select c.channel_name as CHANNEL_NAME,v.video_name as VIDEO_NAME from Youtube.video v,
    Youtube.playlist p,Youtube.channel c where v.playlist_id=p.playlist_id and p.channel_id=c.channel_id''')
    x = cursor.fetchall()

    y = pd.DataFrame(x)
    if (len(y) != 0):
        y = y.set_axis(['CHANNEL NAME', 'VIDEO NAME'], axis='columns')

    st.table(y)


def query2():
    mysql_connection = connect_mysql()
    cursor = mysql_connection.cursor()
    cursor.execute('''select c.channel_name,v.video_count from Youtube.channel c, Youtube.playlist p, 
        (select playlist_id ,a.video_count,dense_rank() over (order by a.video_count desc) as rnk  from (select 
        playlist_id,count(*) as  video_count from Youtube.video group by playlist_id)a)v where v.rnk=1 and 
        p.playlist_id=v.playlist_id and p.channel_id=c.channel_id''')

    x = cursor.fetchall()
    y = pd.DataFrame(x)

    if (len(y) != 0):
        y = y.set_axis(['CHANNEL NAME', 'VIDEO COUNT'], axis='columns')

    st.table(y)


def query3():
    mysql_connection = connect_mysql()
    cursor = mysql_connection.cursor()
    cursor.execute('''select c.channel_name,v.video_name from (select video_name , playlist_id,dense_rank() over (
    order by view_count desc) as rnk  from Youtube.video  )as v, Youtube.channel c, Youtube.playlist p where v.rnk<= 
    10 and v.playlist_id=p.playlist_id and p.channel_id=c.channel_id''')

    x = cursor.fetchall()
    y = pd.DataFrame(x)
    if (len(y) != 0):
        y = y.set_axis(['CHANNEL NAME', 'VIDEO NAME'], axis='columns')

    st.table(y)


def query4():
    mysql_connection = connect_mysql()
    cursor = mysql_connection.cursor()
    cursor.execute('''select video_name,comment_count from youtube.video''')

    x = cursor.fetchall()
    y = pd.DataFrame(x)
    if (len(y) != 0):
        y = y.set_axis(['VIDEO NAME', 'COMMENT COUNT'], axis='columns')

    st.table(y)


def query5():
    mysql_connection = connect_mysql()
    cursor = mysql_connection.cursor()
    cursor.execute('''select v.video_name,c.channel_name from (select video_name , playlist_id,dense_rank() over (
    order by like_count desc) as rnk  from Youtube.video  )as v, Youtube.channel c, Youtube.playlist p where v.rnk = 
    1 and v.playlist_id=p.playlist_id and p.channel_id=c.channel_id''')

    x = cursor.fetchall()
    y = pd.DataFrame(x)
    if (len(y) != 0):
        y = y.set_axis(['VIDEO NAME', 'CHANNEL NAME'], axis='columns')

    st.table(y)


def query6():
    mysql_connection = connect_mysql()
    cursor = mysql_connection.cursor()
    cursor.execute('''select video_name,like_count,dislike_count  from Youtube.video''')

    x = cursor.fetchall()
    y = pd.DataFrame(x)
    if (len(y) != 0):
        y = y.set_axis(['VIDEO NAME', 'LIKE COUNT', 'DISLIKE COUNT'], axis='columns')

    st.table(y)


def query7():
    mysql_connection = connect_mysql()
    cursor = mysql_connection.cursor()
    cursor.execute('''select a.channel_name,sum(a.view_count) as view_count from (select v.playlist_id,v.view_count, 
    c.channel_name,c.channel_id from youtube.video v,youtube.playlist p, Youtube.channel c where 
    c.channel_id=p.channel_id and p.playlist_id=v.playlist_id)a group by a.channel_id,a.channel_name''')

    x = cursor.fetchall()
    y = pd.DataFrame(x)
    if (len(y) != 0):
        y = y.set_axis(['CHANNEL NAME', 'VIEW COUNT'], axis='columns')

    st.table(y)


def query8():
    mysql_connection = connect_mysql()
    cursor = mysql_connection.cursor()
    cursor.execute('''select distinct c.channel_name from Youtube.video v,Youtube.channel c, Youtube.playlist p where 
    year(v.published_date)=2022 and v.playlist_id=p.playlist_id and p.channel_id=c.channel_id''')

    x = cursor.fetchall()
    y = pd.DataFrame(x)
    if (len(y) != 0):
        y = y.set_axis(['CHANNEL NAME'], axis='columns')

    st.table(y)


def query9():
    mysql_connection = connect_mysql()
    cursor = mysql_connection.cursor()
    cursor.execute('''select c.channel_name,avg(v.duration) as average_duration_in_seconds from youtube.channel c, 
    youtube.playlist p,youtube.video v where c.channel_id=p.channel_id and p.playlist_id=v.playlist_id group by 
    c.channel_id,c.channel_name''')

    x = cursor.fetchall()
    y = pd.DataFrame(x)
    if (len(y) != 0):
        y = y.set_axis(['CHANNEL NAME', 'AVERAGE VIDEO DURATION(SECONDS)'], axis='columns')

    st.table(y)


def query10():
    mysql_connection = connect_mysql()
    cursor = mysql_connection.cursor()
    cursor.execute('''select c.channel_name,v.video_name from (select video_name,playlist_id,dense_rank() over (order 
    by comment_count desc) as rnk from youtube.video) v, youtube.channel c, youtube.playlist p where v.rnk=1 and  
    c.channel_id=p.channel_id and p.playlist_id=v.playlist_id''')

    x = cursor.fetchall()
    y = pd.DataFrame(x)
    if (len(y) != 0):
        y = y.set_axis(['CHANNEL NAME', 'VIDEO NAME'], axis='columns')

    st.table(y)


query_list = {'What are the names of all the videos and their corresponding channels?': query1,
              'Which channels have the most number of videos, and how many videos do they have?': query2,
              'What are the top 10 most viewed videos and their respective channels?': query3,
              'How many comments were made on each video, and what are their corresponding video names?': query4,
              'Which videos have the highest number of likes, and what are their corresponding channel names?': query5,
              'What is the total number of likes and dislikes for each video, and what are their corresponding video '
              'names?': query6,
              'What is the total number of views for each channel, and what are their corresponding channel names?': query7,
              'What are the names of all the channels that have published videos in the year 2022?': query8,
              'What is the average duration of all videos in each channel, and what are their corresponding channel names?': query9,
              'Which videos have the highest number of comments, and what are their corresponding channel names?': query10}

query = st.multiselect('Query Selection', query_list.keys(), key='query',
                       help='What do you want to view in output? You can select multiple options')

# if any selection is made in the Query selection dropdown, execute the corresponding query, by calling the corresponding query function
if len(query) != 0:

    for item in query:
        st.write(item)
        query_list[item]()
