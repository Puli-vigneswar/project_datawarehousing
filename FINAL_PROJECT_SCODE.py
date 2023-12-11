#specifying and importing the modules
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#api key initialisation

def api_connect():
    api_id="AIzaSyChZztprL-5KjO43A32U33MhyK5eYEZxjI"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=api_id)

    return youtube
youtube=api_connect()

#extracting channel details
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data= dict(channel_name=i["snippet"]["title"],
                   ch_id=i["id"],                
                subscribers=i['statistics']["subscriberCount"],
                viewes=i['statistics']['viewCount'],
                totalvideos=i["statistics"]['videoCount'],
                channel_desc=i["snippet"]['description'],
                published=i["snippet"]["publishedAt"],
                playlist_id=i["contentDetails"]["relatedPlaylists"]['uploads'])
    return data
        


#get video data

def get_videos_info(channel_id):
    
        video_ids=[]

        response=youtube.channels().list(id=channel_id,
                                        part="contentDetails").execute()

        playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']["uploads"]

        next_page_token=None

        while True:

            response1=youtube.playlistItems().list(
                                                part="snippet",
                                                playlistId=playlist_id,
                                                maxResults=50,
                                                pageToken=next_page_token).execute()
            for i in range(len(response1['items'])):
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response1.get("nextPageToken")
            if next_page_token is None:
                break

        video_info=[]
        for video_id in video_ids:
                request=youtube.videos().list(
                    part="snippet,ContentDetails,statistics",
                    id=video_id
                )
                response=request.execute()

                for item in response["items"]:
                    data=dict(Video_id=item['id'],
                            Title=item["snippet"]['title'],
                            Channel_Id=item['snippet']['channelId'],
                            Channel_name=item["snippet"]["channelTitle"],
                            Definition=item['contentDetails']["definition"],
                            thumbnails=item['snippet']['thumbnails']['default']['url'],
                            Published_Date=item['snippet']['publishedAt'],
                            Duration=item['contentDetails']['duration'],
                            caption=item['contentDetails']['caption'],
                            viewes=item["statistics"].get('viewCount'),
                            likes=item["statistics"].get('likeCount'),
                            Comments=item['statistics'].get('commentCount')
                            )
                    video_info.append(data)        
        return video_info

#extracting comments data
#get video and its ids
def get_cmnts_info(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part="contentDetails").execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']["uploads"]
    next_page_token=None
    while True:
        response1=youtube.playlistItems().list(
                                            part="snippet",
                                            playlistId=playlist_id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get("nextPageToken")
        if next_page_token is None:
            break
    video_ids
    
    comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100
            )
        response=request.execute()

        for item in response['items']:
            data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                    videoId=item['snippet']['topLevelComment']['snippet']["videoId"],
                    comment=item['snippet']["topLevelComment"]["snippet"]["textDisplay"],
                    comment_author=item['snippet']["topLevelComment"]["snippet"]["authorDisplayName"])
                                
            comment_data.append(data)
    except:
        pass
    return comment_data


#loading data to mongodb 
client=pymongo.MongoClient("mongodb+srv://dbpuli:dbvigneswar@cluster0.xkgqa47.mongodb.net/?retryWrites=true&w=majority")
db=client['scrapped_data']

def channel_details(channel_id):
    chnl_info=get_channel_info(channel_id)
    vi_info=get_videos_info(channel_id)
    cmnts_info=get_cmnts_info(channel_id)

    coll=db["CHANNEL_DETAILS"]
    coll.insert_one({"channel_info":chnl_info,
                 "videos_info":vi_info,
                 "cmnts_info":cmnts_info})
    return "uploaded successfully"

channels=["UCdF7DdIgrklXGDL-DzcOLnQ",
          "UCoUwmxg6abrDguN7PsM0kMw",
          "UCnCak07qLg38glTF5GVjbyQ",
          "UCI9RhJN_RglbKmRKgZgkpNA",
          "UC9op3zPJFIM2rYd9XynTeAg",
          "UCoy1_qI_MO3NdprrTCdwS2A",
          "UCFI1eIuG58Cv8tr5yrUvHKQ",
          "UCWMfxHgCT8tneezeEey6qNg",
          "UCF3v18Z-AHeN1JjvZCGLiGA",
          "UCz-zYuWLC8gWuxzm-jTjCcA"]
#uncomment below lines to add data to mongodb
#for i in channels:
 #   insert=channel_details(i)'''

#initiating sql tables for channels,playlists,videos,comment details
def channel_table():
                        mydb=psycopg2.connect(host="localhost",
                                                user="postgres",
                                                password="123456789",
                                                database="YOUTUBE_DATA",
                                                port="5432")
                        cursor=mydb.cursor()
                        drop_query='''drop table if  exists channels'''
                        cursor.execute(drop_query)
                        mydb.commit()
                        try:
                                create_query='''create table if not exists channels(channel_name varchar(100),
                                                                                        ch_id varchar(50) primary key,
                                                                                        subscribers bigint,
                                                                                        viewes bigint,
                                                                                        totalvideos bigint,
                                                                                        channel_desc text,
                                                                                        published timestamp,
                                                                                        Playlist_id varchar(100))'''
                                cursor.execute(create_query)
                                mydb.commit()
                        except:
                                print("Channels table already created")
                        #initialising pandas data frame
                        chnl_list=[]
                        db=client["scrapped_data"]
                        coll=db['CHANNEL_DETAILS']
                        for ch_data in coll.find({},{"_id":0,"channel_info":1}):
                                chnl_list.append(ch_data["channel_info"])
                        df=pd.DataFrame(chnl_list)
                        for index,row in df.iterrows():
                                insert_query='''INSERT INTO channels(channel_name ,
                                                                ch_id,
                                                                subscribers,
                                                                viewes,
                                                                totalvideos,
                                                                channel_desc,
                                                                published,
                                                                Playlist_id)
                                                                                values(%s,%s,%s,%s,%s,%s,%s,%s)'''                               
                                values=(row["channel_name"],
                                        row["ch_id"],
                                        row['subscribers'],
                                        row['viewes'],
                                        row['totalvideos'],
                                        row['channel_desc'],
                                        row['published'],
                                        row['playlist_id'])
                                try:
                                        cursor.execute(insert_query,values)
                                        mydb.commit()
                                except:
                                        print("channel values already inserted")             

                            #creating sql videos table
def video_table():
                mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="123456789",
                        database="YOUTUBE_DATA",
                        port="5432")
                cursor=mydb.cursor()
                drop_query='''drop table if exists videos'''
                cursor.execute(drop_query)
                mydb.commit()
                create_query='''create table if not exists videos(Video_id varchar (100) primary key,
                                                                Title varchar(100),
                                                                channel_id varchar(100) ,
                                                                channel_name varchar(100),
                                                                Definition varchar(100),
                                                                thumbnails varchar(100),
                                                                Published_date timestamp,
                                                                Duration INTERVAL,
                                                                caption varchar(100),
                                                                viewes bigint,
                                                                likes bigint,
                                                                comments bigint)'''
                cursor.execute(create_query)
                mydb.commit()
                vi_list=[]
                db=client['scrapped_data']
                coll=db["CHANNEL_DETAILS"]
                for vi_data in coll.find({},{'_id':0,"videos_info":1}):
                    for i in range(len(vi_data["videos_info"])):
                        vi_list.append(vi_data["videos_info"][i])
                df=pd.DataFrame(vi_list)
                for index,row in df.iterrows():
                    insert_query='''insert into videos(Video_id,
                                        Title,
                                        Channel_Id,
                                        Channel_name,
                                        Definition,
                                        thumbnails,
                                        Published_Date,
                                        Duration,
                                        caption,
                                        viewes,
                                        likes,
                                        Comments)
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''                               
                    values=(row['Video_id'],
                            row['Title'],
                            row['Channel_Id'],
                            row['Channel_name'],
                            row['Definition'],
                            row['thumbnails'],
                            row['Published_Date'],
                            row['Duration'],
                            row['caption'],
                            row['viewes'],
                            row['likes'],
                            row['Comments']
                            )
                    try:
                        cursor.execute(insert_query,values)
                        mydb.commit()
                    except:
                        print("videos values already inserted")

def comments_table():
                        mydb=psycopg2.connect(host="localhost",
                                                user="postgres",
                                                password="123456789",
                                                database="YOUTUBE_DATA",
                                                port="5432")
                        cursor=mydb.cursor()
                        drop_query='''drop table if exists comments'''
                        cursor.execute(drop_query)
                        mydb.commit()
                        create_query='''create table if not exists comments(comment_id varchar(50),
                                                                            videoId varchar(50),
                                                                            comment text,
                                                                            comment_author varchar(80))'''
                        cursor.execute(create_query)
                        mydb.commit()
                        cmnts_list=[]
                        db=client['scrapped_data']
                        coll=db["CHANNEL_DETAILS"]
                        for comment in coll.find({},{'_id':0,"cmnts_info":1}):
                            for i in range(len(comment["cmnts_info"])):
                                cmnts_list.append(comment["cmnts_info"][i])
                        df=pd.DataFrame(cmnts_list)

                        for index,row in df.iterrows():
                            insert_query='''insert into comments(comment_id,
                                                                videoId,
                                                                comment,
                                                                comment_author
                                                                )
                                                                values(%s,%s,%s,%s)'''                               
                            values=(row['comment_id'],
                                    row['videoId'],
                                    row['comment'],
                                    row['comment_author'])
                            try:
                                cursor.execute(insert_query,values)
                                mydb.commit()
                            except:
                                print("comment values already inserted")


                        #tables created successfully
def tables():
    channel_table()
    video_table()
    comments_table()
    return "created"
table=tables()

#initialising pandas data frame for streamlit
def get_channel_table():
    chnl_list=[]
    db=client["scrapped_data"]
    coll=db['CHANNEL_DETAILS']
    for ch_data in coll.find({},{"_id":0,"channel_info":1}):
        chnl_list.append(ch_data["channel_info"])
    df1=st.dataframe(chnl_list)   
    return df1
def get_video_table():
    vi_list=[]
    db=client['scrapped_data']
    coll=db["CHANNEL_DETAILS"]
    for vi_data in coll.find({},{'_id':0,"videos_info":1}):
        for i in range(len(vi_data["videos_info"])):
            vi_list.append(vi_data["videos_info"][i])
    df2=st.dataframe(vi_list)
    return df2 

def get_comments_table():
    cmnts_list=[]
    db=client['scrapped_data']
    coll=db["CHANNEL_DETAILS"]
    for comment in coll.find({},{'_id':0,"cmnts_info":1}):
        for i in range(len(comment["cmnts_info"])):
            cmnts_list.append(comment["cmnts_info"][i])
    df3=st.dataframe(cmnts_list)
    return df3  
                    #streamlit code
with st.sidebar:
    st.title(':orange[YOUTUBE DATA HARVESTING AND WAREHOUSING]')
    st.header('Skill take Away')
    st.caption("Python scripting")
    st.caption("Data harvesting")
    st.caption("MONGODB")
    st.caption("API")
    st.caption("Data Management using MongoDB and sql")
            #getting channel is as input from the user
channel_id=st.text_input("Enter the CHANNEL ID")

if st.button("Collect and Store Data to Mongodb Server"):
      ch_ids=[]
      db=client['scrapped_data']
      coll=db["CHANNEL_DETAILS"]
      for ch_data in coll.find({},{"_id":0,"channel_info":1}):
                ch_ids.append(ch_data["channel_info"]["ch_id"])
                
      if channel_id in ch_ids:
          st.success("Channel Details of the given channel id already existed")
      else:
            insert =channel_details(channel_id)
            st.success(insert)
if st.button("MIGRATE  DATA TO SQL"):
      table=tables()
      st.success(table)

get_table=st.radio("choose the TABLE to explore",("CHANNELS","VIDEOS","COMMENTS"))

if get_table=="CHANNELS":
      get_channel_table()
elif get_table=="VIDEOS":
      get_video_table()
elif get_table=="COMMENTS":
      get_comments_table()

#binding sql
mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="123456789",
                        database="YOUTUBE_DATA",
                        port="5432")
cursor=mydb.cursor()

Query =st.selectbox("Select your query",("1.NAME OF THEIR CHANNELS AND THEIR CORRESPONDING VIDEOS DETAILS",
                                         "2.CHANNELS WITH MOST NUMBER OF VIDEOS AND HOW MANY VIDEOS DO THEY HAVE?",
                                         "3.WHAT ARE THE TOP 10 MOST VIEWED VIDEOS AND THEIR RESPECTIVE CHANNELS?",
                                         "4.HOW MANY COMMENTS MADE ON EACH VIDEO,AND THEIR CORRESPONDING VIDEO NAMES?",
                                         "5.WHICH VIDEOS HAVE THE MOST NUMBER OF LIKES AND THEIR CORRESPONDING CHANNELS?",
                                         "6.WHAT ARE THE TOTAL NUMBER OF LIKES FOR EACH VIDEO,AND THE VIDEO NAME?",
                                         "7.TOTAL NUMBER OF VIEWS FOR EACH CHANNEL AND CHANNEL NAMES? ",
                                         "8.VIDEOS WHICH WERE UPLOADED IN THE YEAR OF 2022?",
                                         "9.AVERAGE DURATION OF ALL VIDEOS IN EACH CHANNEL AND THEIR CORRESPONDING CHANNEL NAMES?",
                                         "10.WHICH VIDEOS HAVE THE HIGHEST NUMBER OF COMMENTS AND THEIR CORRESPONDING CHANNEL NAMES?"
                                    
                                         ))

                #scroll bar questions defined
mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="123456789",
                        database="YOUTUBE_DATA",
                        port="5432")
cursor=mydb.cursor()
if Query=="1.NAME OF THEIR CHANNELS AND THEIR CORRESPONDING V…":
    Query1='''select Title as videos,channel_name as channelname from videos'''
    cursor.execute(Query1)
    mydb.commit()
    q1=cursor.fetchall()
    df1=pd.DataFrame(q1,columns=["Video Title","Channel"])
    st.write(df1)

elif Query=="2.CHANNELS WITH MOST NUMBER OF VIDEOS AND HOW MANY VIDEOS DO THEY HAVE?":
    Query2='''select channel_name as CHANNELS, totalvideos as TOTALVIDEOS from channels order by totalvideos desc'''
    cursor.execute(Query2)  
    mydb.commit()
    q2=cursor.fetchall()
    df2=pd.DataFrame(q2,columns=["CHANNELS","TOTALVIDEOS"])
    st.write(df2)

elif Query=="3.WHAT ARE THE TOP 10 MOST VIEWED VIDEOS AND THEIR RESPECTIVE CHANNELS?":
    Query3='''select Title as Title,viewes as VIEWS, channel_name as CHANNEL from videos 
                where viewes is not null order by viewes desc limit 10'''
    cursor.execute(Query3)  
    mydb.commit()
    q3=cursor.fetchall()
    df3=pd.DataFrame(q3,columns=["Title","VIEWS","CHANNEL"])
    st.write(df3)
elif Query=="4.HOW MANY COMMENTS MADE ON EACH VIDEO,AND THEIR CORRESPONDING VIDEO NAMES":
      query4='''select Title as Title,comments as Totalcomments from videos'''
      cursor.execute(query4)
      mydb.commit()
      q4=cursor.fetchall()
      df4=pd.DataFrame(q4,columns=["Title","Totalcomments"])
      st.write(df4)
elif Query=="5.WHICH VIDEOS HAVE THE MOST NUMBER OF LIKES AND THEIR CORRESPONDING CHANNELS?":
      Query5='''select Title as Title,likes as LIKES,channel_name as CHANNEL from videos order by likes desc'''
      cursor.execute(Query5)  
      mydb.commit()
      q5=cursor.fetchall()
      df5=pd.DataFrame(q5,columns=["Title","LIKES","CHANNEL"])
      st.write(df5)
elif Query=="6.WHAT ARE THE TOTAL NUMBER OF LIKES FOR EACH VIDEO,AND THE VIDEO NAME?":
      #dislikes has been disabled
      Query6='''select Title as VIDEO,likes as Likes from videos'''
      cursor.execute(Query6)
      mydb.commit()
      q6=cursor.fetchall()
      df6=pd.DataFrame(q6,columns=["VIDEO","Likes"])
      st.write(df6) 
elif Query=="7.TOTAL NUMBER OF VIEWS FOR EACH CHANNEL AND CHANNEL NAMES?":
      Query7='''select channel_name as CHANNELS, viewes as VIEWS from channels order by viewes desc'''
      cursor.execute(Query7)
      mydb.commit()
      q7=cursor.fetchall()
      df7=pd.DataFrame(q7,columns=["CHANNELS","VIEWS"])
      st.write(df7)
elif Query=="8.VIDEOS WHICH WERE UPLOADED IN THE YEAR OF 2022?":
      Query8='''select channel_name as CHANNEL,Title as VIDEO ,Published_date as UPLOADED_IN from videos
             where extract (year from Published_date)=2022'''
      cursor.execute(Query8)
      mydb.commit()
      q8=cursor.fetchall()
      df8=pd.DataFrame(q8,columns=["CHANNEL","VIDEO","UPLOADED_IN"])
      st.write(df8)
elif Query=="9.AVERAGE DURATION OF ALL VIDEOS IN EACH CHANNEL AND THEIR CORRESPONDING CHANNEL NAMES":
      Query9='''select channel_name as CHANNEL,AVG(duration) as Averageduration from videos
                group by channel_name'''
      cursor.execute(Query9)
      mydb.commit()
      q9=cursor.fetchall()
      df9=pd.DataFrame(q9,columns=["CHANNEL","Averageduration"])
      st.write(df9)
elif Query=="10.WHICH VIDEOS HAVE THE HIGHEST NUMBER OF COMMENTS AND THEIR CORRESPONDING CHANNEL NAMES?":
      Query10='''select Title as VIDEO, channel_name as CHANNEL,comments as COMMENTS 
      from videos where comments is not null order by comments desc '''
      cursor.execute(Query10)
      mydb.commit()
      q10=cursor.fetchall()
      df10=pd.DataFrame(q10,columns=["VIDEO","CHANNEL","COMMENTS"])
      st.write(df10)

                                                     #EOC
                        

      
      


