import streamlit as st
from googleapiclient.discovery import build
from pymongo import MongoClient
import psycopg2
import json

st.title("Youtube Data Extractor")

api_key = "AIzaSyDxyy6JxuPh34TsY5_6QE4FS3mGEEnk1Cw"

connection_string = "mongodb+srv://GuviAPIProject:sat123@etlprocess.w9ytg5q.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connection_string)
db = client["youtube_data_lake"]
collection = db["channel_data"]

pg_conn = psycopg2.connect(
    database="YoutubeAPI",
    user="postgres",
    password="sat123",
    host="localhost",
    port="5432"
)
mongo_data = collection.find()

channel_url = st.text_input("Channel URl")

if not channel_url:
    st.warning("Please provide Channel URL.")
else:
    youtube = build("youtube", "v3", developerKey=api_key)

    def get_channel_stats(youtube, channel_url):

        request = youtube.channels().list(
            part="snippet, contentDetails, statistics",
            id=channel_url
        )

        channel_data = request.execute()

        if 'items' in channel_data:
            channel_info = {
                "Channel_Name": channel_data['items'][0]["snippet"]["title"],
                "Channel_Id": channel_data['items'][0]["id"],
                "Subscription_Count": channel_data["items"][0]["statistics"]["subscriberCount"],
                "Channel_Views": channel_data["items"][0]["statistics"]["viewCount"],
                "Channel_Description": channel_data["items"][0]["snippet"]["description"],
                "Playlist_Id": channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            }
            return channel_info
        else:
            return None

    channel_informations = get_channel_stats(youtube, channel_url)

    if channel_informations is not None:
        playlist_id = channel_informations.get("Playlist_Id", "N/A")

        def video_ids(youtube, playlist_id):
            video_ids = []

            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50)

            playlist_data = request.execute()

            while 'items' in playlist_data:
                for i in range(len(playlist_data["items"])):
                    ids = playlist_data["items"][i]["contentDetails"]["videoId"]
                    video_ids.append(ids)

                if 'nextPageToken' in playlist_data:
                    request = youtube.playlistItems().list(
                        part="contentDetails",
                        playlistId=playlist_id,
                        maxResults=50,
                        pageToken=playlist_data["nextPageToken"])
                    playlist_data = request.execute()
                else:
                    break

            return video_ids

        video_ids_url = video_ids(youtube, playlist_id)

    else:
        st.error("Failed to retrieve channel information. Check your API key and URL.")
        video_ids_url = []

    def get_all_comments(youtube, video_ids_url):
        all_comments = []
        page_token = None

        try:
            while True:
                comments_request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_ids_url,
                    pageToken=page_token
                )
                comments_data = comments_request.execute()

                for comment_item in comments_data.get("items", []):
                    comment_info = {
                        "Comment_Id": comment_item['snippet']['topLevelComment']['id'],
                        "Comment_Text": comment_item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        "Comment_Author": comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        "Comment_PublishedAt": comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
                    all_comments.append(comment_info)

                page_token = comments_data.get("nextPageToken")
                if not page_token:
                    break  # Exit the loop when there are no more comments

        except Exception as e:
            st.error(f"An error occurred while retrieving comments: {str(e)}")

        comment_details = [f'"Comment_Id_{x}": {comment_dts}' for x, comment_dts in enumerate(all_comments, start=1)]

        formatted_comment_string = ", ".join(comment_details)

        return formatted_comment_string

    def get_video_details(youtube, video_ids_url):
        all_video_data = []

        for video_id in video_ids_url:
            video_request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id)
            video_data = video_request.execute()

            snippet = video_data['items'][0]["snippet"]
            statistics = video_data['items'][0]["statistics"]
            content_details = video_data['items'][0]["contentDetails"]

            video_comments = get_all_comments(youtube, video_id)

            video_info = {
                "Video_Id": video_id,
                "Video_Name": snippet["title"],
                "Video_Description": snippet["description"],
                "Tags": "Not Available" if snippet.get("tags") is None else snippet["tags"],
                "Published_At": snippet["publishedAt"],
                "View_Count": statistics.get("viewCount"),
                "Like_Count": statistics.get("likeCount"),
                "Dislike_Count": statistics.get("dislikeCount"),
                "Favorite_Count": statistics.get("favoriteCount"),
                "Comment_Count": statistics.get("commentCount"),
                "Duration": content_details["duration"],
                "Thumbnail": snippet["thumbnails"]["default"]["url"],
                "Caption_Status": "Available" if content_details["caption"] == "true" else "Not Available",
                "Comments": {video_comments}
            }
            all_video_data.append(video_info)

        video_details_strings = [f'"Video_Id_{x}": {video_dts}' for x, video_dts in enumerate(all_video_data, start=1)]

        formatted_string = ",".join(video_details_strings)

        return formatted_string

    channel_video_comment_datas = get_video_details(youtube, video_ids_url)

    result = {
        "Channel_Name": channel_informations,
        "Playlist_Url": channel_informations.get("Playlist_Id", "N/A"),
        "Video_Details": channel_video_comment_datas
    }

    extracted_data = None

    if st.button("Extract Data"):
        extracted_data = result
        st.write(extracted_data)

    if st.button("Push to MongoDB"):
        if isinstance(result, dict):
            collection.insert_one(result)
            st.success("Data Stored in MongoDB Successfully")
        else:
            print("extracted_data is not a dictionary. Please ensure it's in the correct format.")


def transfer_data():
    try:
        cursor = pg_conn.cursor()
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS Channel_Table (
            Channel_Name TEXT,
            Playlist_URL TEXT,
            Video_Details TEXT
        );
        '''
        cursor.execute(create_table_query)
        pg_conn.commit()

        for document in mongo_data:

            channel_name_json = json.dumps(document.get("Channel_Name"))

            playlist_url = json.dumps(document.get("Playlist_URL"))

            video_details_json = json.dumps(document.get("Video_Details"))

            cursor.execute('INSERT INTO Channel_Table (Channel_Name, Playlist_URL, Video_Details) VALUES (%s, %s, %s);',
                           (channel_name_json, playlist_url, video_details_json))
            pg_conn.commit()

        cursor.close()
    except Exception as e:
        st.error(f"Error transferring data: {str(e)}")


# Streamlit app starts here
st.title("MongoDB to PostgreSQL Data Transfer")

if st.button("Transfer Data from MongoDB to PostgreSQL"):
    st.write("Transferring data...")
    transfer_data()
    st.success("Data transfer completed!")

st.title("PostgreSQL Table Viewer")


def display_postgresql_table():
    try:
        cursor = pg_conn.cursor()
        cursor.execute("SELECT * FROM Channel_Table;")
        rows = cursor.fetchall()

        if rows:
            st.write("PostgreSQL Table:")
            for row in rows:
                st.write(row)
        else:
            st.warning("No data in the PostgreSQL table.")

        cursor.close()
    except Exception as e:
        st.error(f"Error fetching data from PostgreSQL: {str(e)}")


if st.button("Fetch and Display PostgreSQL Table"):
    st.write("Fetching data from PostgreSQL...")
    display_postgresql_table()


client.close()
pg_conn.close()
