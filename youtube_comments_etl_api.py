import os
from googleapiclient.discovery import build
import pandas as pd
import s3fs


def run_youtube_etl():
    # Set your API key here
    DEVELOPER_KEY = "AIzaSyDXm7fsa2ggiz_XHPQT7ePOjHBZoY95v84"
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"

    # Video ID to fetch comments from
    video_id = "q8q3OFFfY6c"  # Replace with your own if needed
    youtube = build(YOUTUBE_API_SERVICE_NAME,
                    YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)

    comments = []
    next_page_token = None

    while True:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id,
            maxResults=100,  # Max allowed
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response["items"]:
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            comment_data = {
                "author": snippet["authorDisplayName"],
                "published_at": snippet["publishedAt"],
                "comment": snippet["textDisplay"]
            }
            comments.append(comment_data)

        next_page_token = response.get("nextPageToken")

        if not next_page_token:
            break  # No more pages

    df = pd.DataFrame(comments)
    df.to_csv("youtube_comments.csv", index=False)
