from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
import streamlit as st
from datetime import datetime
import time
from dateutil import parser
import dateutil.parser
import re


#youtube_API connect
def Api_connect():
    api_key = "AIzaSyDzl66H0MC5G9UZ8HU0b3BxdOOIYl7ERAA"
    youtube = build('youtube', 'v3', developerKey=api_key)
    return youtube
youtube=Api_connect()

#Time formation
def time_parse(duration):

    matches = re.match(r'PT(\d+M)?(\d+S)?', duration)
    #Extract minutes and seconds from the matched groups
    duration_minutes = int(matches.group(1)[:-1]) if matches.group(1) else 0
    duration_seconds = int(matches.group(2)[:-1]) if matches.group(2) else 0

    # Convert duration to total seconds
    duration_in_seconds = duration_minutes * 60 + duration_seconds

    # Convert total seconds to hours, minutes, and seconds
    hours, remainder = divmod(duration_in_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_duration = "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))

    return formatted_duration



#CHANNEL INFORMATION
def get_Channelinfo(Channel_id):
    
    request= youtube.channels().list(
        part="Snippet,ContentDetails,statistics",
        id = Channel_id
    )
    response = request.execute()
    
    for i in range(len(response['items'])):
        data = dict(Channel_name = response['items'][i]['snippet']['title'],
                    Channel_Id = response["items"][i]["id"],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
    return data  

#VIDEO ID INFORMATION
def get_channel_videos(channel_id):
   
    video_ids = []
    res = youtube.channels().list(id=channel_id, 
                                    part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
        
    while True:
        res = youtube.playlistItems().list( 
                                            part = 'snippet',
                                            playlistId = playlist_id, 
                                            maxResults = 50,
                                            pageToken = next_page_token).execute()
            
        for i in range(len(res['items'])):
                video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
            
        if next_page_token is None:
            break
    return video_ids

#VIDEO INFORMATION
def get_video_info(video_ids):
    
    video_data = []
    for video_id in video_ids:
            request = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id= video_id)
            response = request.execute()

            for item in response["items"]:
                data = dict(Channel_Name = item['snippet']['channelTitle'],
                            Channel_Id = item['snippet']['channelId'],
                            Video_Id = item['id'],
                            Title = item['snippet']['title'],
                            Tags = item['snippet'].get('tags'),
                            Thumbnail = item['snippet']['thumbnails']['default']['url'],
                            Description = item['snippet']['description'],
                            Published_Date = item['snippet']['publishedAt'],
                            Duration = time_parse(item['contentDetails']['duration']),
                            Views = item['statistics']['viewCount'],
                            Likes = item['statistics'].get('likeCount'),
                            Comments = item['statistics'].get('commentCount'),
                            Favorite_Count = item['statistics']['favoriteCount'],
                            Definition = item['contentDetails']['definition'],
                            Caption_Status = item['contentDetails']['caption']
                            )
                video_data.append(data)
    return video_data

#COMMAND INFORMATION
def get_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass
              
        return Comment_Information    

#PLAYLIST INFORMATION
def get_Playlist_details(channel_id):
        next_page_token = None
        All_data=[]
        while True:
                request = youtube.playlists().list( 
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response = request.execute()

                for item in response['items']:
                        data = dict(Playlist_Id=item['id'],
                                        Title=item['snippet']['title'],
                                        channelId=item['snippet']['channelId'],
                                        channel_Name=item['snippet']['channelTitle'],
                                        PublishedAt=item['snippet']['publishedAt'],
                                        Video_count=item['contentDetails']['itemCount'])
                        All_data.append(data)
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break 
        return All_data


#CONNECTION WITH MONGODB
client=pymongo.MongoClient("mongodb+srv://pkarthika923:karthikamongo@cluster0.iychizj.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_data"]

#UPLOADING DATA TO MONGODB
def channel_details(channel_id):

    ch_details=get_Channelinfo(channel_id)
    pl_details=get_Playlist_details(channel_id)
    vi_ids=get_channel_videos(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db['channel_details']
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                    "video_information":vi_details,"comment_information":com_details})

    return "Uploaded successfully"

#TABLE CREATION/DATA INSERTION FOR CHANNEL
def channels_table():
    mydb = pymysql.connect(host="127.0.0.1",
                    user="root",
                    password="admin@123",
                    database= "youtube_data"
                    )
    cursor = mydb.cursor()
    
    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)

    try:

        create_query = '''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key, 
                                                        Subscription_Count bigint, 
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(50))'''
        cursor.execute(create_query)
        
    except:
        print("Channel table already created")

    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        
        insert_query = '''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscription_Count,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id )
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
                                            
        values = (row['Channel_name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_videos'],
                row['Description'],
                row['playlist_id'])
        
        try:
            cursor.execute(insert_query,values)  
            mydb.commit()      
        except:
            print("Channels values are already inserted")

#TABLE CREATION/DATA INSERTION VIDEO DETAILS
def videos_table():

    mydb = pymysql.connect(host="127.0.0.1",
                    user="root",
                    password="admin@123",
                    database= "youtube_data"
                    )
    cursor = mydb.cursor()
    

    drop_query='''drop table if exists videosdata'''
    cursor.execute(drop_query)
    try:
        create_query = '''create table if not exists videosdata(Channel_Name varchar(150),
                                                            Channel_Id varchar(150),
                                                            Video_Id varchar(75), 
                                                            Title varchar(500), 
                                                            Tags text,
                                                            Thumbnail varchar(400),
                                                            Description text, 
                                                            Published_Date varchar(150),
                                                            Duration time,
                                                            Views int, 
                                                            Likes int,
                                                            Comments int,
                                                            Favorite_Count int, 
                                                            Definition varchar(50), 
                                                            Caption_Status varchar(50) 
                                )''' 
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Video details are already created")
    

#TABLE CREATION/DATA INSERTION VIDEO TABLE
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)


    for index,row in df2.iterrows():
        test= str(row['Tags']).replace('[','').replace(']','')
    
        insert_query ='''insert into videosdata(Channel_Name,
                                            Channel_Id,
                                            Video_Id, 
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favorite_Count,
                                            Definition,
                                            Caption_Status)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                row['Channel_Id'], 
                row['Video_Id'], 
                row['Title'], 
                test,
                row['Thumbnail'], 
                row['Description'], 
                row['Published_Date'], 
                row['Duration'], 
                row['Views'], 
                row['Likes'], 
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'], 
                row['Caption_Status'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("Video details are already inserted")


#TABLE CREATION/DATA INSERTION FOR PLAYLIST
def playlist_table():
    mydb = pymysql.connect(host="127.0.0.1",
                    user="root",
                    password="admin@123",
                    database= "youtube_data"
                    )
    cursor = mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)

    try:
        create_query = '''CREATE TABLE IF NOT EXISTS playlists (PlaylistId VARCHAR(100) PRIMARY KEY,
                                                                Title VARCHAR(80),
                                                                ChannelId VARCHAR(100),
                                                                ChannelName VARCHAR(100),
                                                                PublishedAt VARCHAR(50),
                                                                VideoCount INT
                                    )
        '''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Play list values are already created")
    


    db = client["Youtube_data"]
    coll1 =db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    df = pd.DataFrame(pl_list)
    try:
        for index,row in df.iterrows():
            insert_query = '''INSERT into playlists(PlaylistId,
                                                        Title,
                                                        ChannelId,
                                                        ChannelName,
                                                        PublishedAt,
                                                        VideoCount)
                                            VALUES(%s,%s,%s,%s,%s,%s)'''            
            values =(
                    row['Playlist_Id'],
                    row['Title'],
                    row['channelId'],
                    row['channel_Name'],
                    row['PublishedAt'],
                    row['Video_count'])
                    
            cursor.execute(insert_query,values)
            mydb.commit()

    except:
            print("Playlist details are already inserted")


#TABLE CREATION/DATA INSERTION COMMENTS TABLE
def comments_table():
    mydb = pymysql.connect(host="127.0.0.1",
                user="root",
                password="admin@123",
                database= "youtube_data"
                )
    cursor = mydb.cursor()
 
    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    try:
        create_query = '''create table if not exists comments(Comment_Id varchar(100) PRIMARY KEY,
                                            Video_Id varchar(50),
                                            Comment_Text text,
                                            Comment_Author varchar(150),
                                            Comment_Published varchar(50))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
         print("Comments details are already inserted")


    cm_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for cm_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(cm_data["comment_information"])):
            cm_list.append(cm_data["comment_information"][i])
            df3=pd.DataFrame(cm_list)
            
            for index,row in df3.iterrows():
                insert_query = '''insert into comments(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published)
                                            values(%s,%s,%s,%s,%s)'''
                                            
                values = (row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published']
                )
              
                try:
                    print(insert_query,values)
                    cursor.execute(insert_query,values)
                    mydb.commit()
                except:
                    print("Comments are already inserted")

#COMMON FUNCTION FOR ALL TABLES CREATION/INSERTION
def tables():
    channels_table()
    videos_table()
    playlist_table()
    comments_table()

    return "Table created successfully"

#STREAMLIT VIEW FOR CHANNEL
def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)
    return df

#STREAMLIT VIEW FOR PLAYLIST
def show_playlist_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)
    return df1

#STREAMLIT VIEW FOR VIDEOS
def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)
    return df2

#STREAMLIT VIEW FOR COMMENTS
def show_comments_table():
    cm_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for cm_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(cm_data["comment_information"])):
            cm_list.append(cm_data["comment_information"][i])
    df3=st.dataframe(cm_list)
    return df3

#output - streamlit
#UI
st.title(":red[ YouTube DATA HARVESTING AND WAREHOUSING]")
with st.sidebar:
    
    st.header(":black[Skill take away]")
    st.caption("Data collection from API")
    st.caption("Python Scripting")
    st.caption("Data Organizing in MONGO DB")
    st.caption("Store the data")
    st.caption("Data management")


#CHECK/FORMATTING CHANNELS IDS
channel_id=st.text_input("Enter the CHANNEL ID")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and store data"):

    for channel in channels:
        if channel=="":
            st.success("Please enter a valid CHANNEL ID")
        else:
            ch_ids = []
            db=client["Youtube_data"]
            coll1=db["channel_details"]
            for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
                ch_ids.append(ch_data['channel_information']['Channel_Id'])

            if channel_id in ch_ids:
                st.success("Entered CHANNEL ID details already exists")
            else:
                insert=channel_details(channel)
                st.success(insert)


     
#ACTION MIGRATE TO SQL
if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)

#VIEW TABLES
show_tables=st.radio("Please select the below options to view in table",("Channels","Playlists","Videos","Comments"))

if show_tables=="Channels":
    show_channels_table()
elif show_tables=="Playlists":
    show_playlist_table()
elif show_tables=="Videos":   
    show_videos_table()
elif show_tables=="Comments":
    show_comments_table()

#sql connection-10 question
mydb = pymysql.connect(host="127.0.0.1",
                user="root",
                password="admin@123",
                database= "youtube_data"
                )
cursor = mydb.cursor()

question = st.selectbox('Please Select Your Question',
                        ('1.Show all the videos and the Channel Name',
                        '2. View the channels with most number of videos',
                        '3. View the 10 most viewed videos',
                        '4. View the Comments in each video',
                        '5. List the Videos with highest likes',
                        '6. List Likes of all videos',
                        '7. Show the Views of each channel',
                        '8. Filter the Videos published in the year 2022',
                        '9. Calculate the average duration of all videos in each channel',
                        '10.Show the videos with highest number of comments'))
#Questions/query
if question=="1.Show all the videos and the Channel Name":
    query1 = ''' select Channel_Name as channels ,Title as videos from videosdata'''
    cursor.execute(query1)
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=["video","channel_name"])
    st.write(df1)

elif question=="2. View the channels with most number of videos":
    query2 = ''' select Channel_Name as channels ,Total_Videos as total_videos from channels order by total_videos desc'''
    cursor.execute(query2)
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["Channel_Name","No of videos"])
    st.write(df2)

elif question==("3. View 10 most viewed videos"):
    query3 = ''' select Channel_Name as channels,Title as videos,Views as views from videosdata where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["Channel_Name","Video","Views"])   
    st.write(df3)

elif question==("4. View Comments in each video"):
    query4 = ''' select Channel_Name as channels,Title as videos,Comments as comments from videosdata where comments is not null'''
    cursor.execute(query4)
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["Channel_Name","Video","Comments_count"])
    st.write(df4)

elif question==("5. List the Videos with highest likes"):
    query5 = '''select Channel_Name ,Title ,Likes as like_count from videosdata where Likes is not null order by like_count desc'''
    cursor.execute(query5)
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["Channel_Name","Title","like_count"])
    st.write(df5)

elif question=="6.List Likes of all videos":
    query6 = '''select Title ,Likes as like_count from videosdata where Likes is not null'''
    cursor.execute(query6)
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["Title","like_count"])
    st.write(df6)

elif question=="7.Show the Views of each channel":
    query7 = '''select Channel_Name as name,Views as view_count from channels where Views is not null'''
    cursor.execute(query7)
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["Channel_Name","view_count"])
    st.write(df7)

elif question=="8. Filter the Videos published in the year 2022":
    query8 = '''select Title as video_title,Published_Date as date,Channel_Name from videosdata where extract(year from Published_date)=2022'''
    cursor.execute(query8)
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Video_Title","Published_Date","Channel_Name"])
    st.write(df8)
    
elif question=="9. Calculate the average duration of all videos in each channel":
    query9 = ''' select Channel_Name as channelname, SEC_TO_TIME(AVG(TIME_TO_SEC(Duration))) as avgduration from videosdata group by Channel_Name'''
    cursor.execute(query9)
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["Channel_Name","Duration_AVG"])

    TF9=[]
    for index,row in df9.iterrows():
        channel_title=row["Channel_Name"]
        avg_duration=row["Duration_AVG"]
        avg_duration_str=str(avg_duration)
        TF9.append(dict(Channel_Title=channel_title,Avg_duartion=avg_duration_str))
    df_str=pd.DataFrame(TF9)
    st.write(df_str)


elif question=="10.Show the videos with highest number of comments":
    query10 = ''' select Channel_Name as channels,Title as videos,Comments as comments from videosdata where comments is not null order by comments desc'''
    cursor.execute(query10)
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["Channel_Name","Video","Comments_count"])
    st.write(df10)