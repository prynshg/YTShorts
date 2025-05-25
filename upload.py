import os
import gspread
import pandas as pd
import requests
from datetime import datetime
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from gspread_dataframe import set_with_dataframe

# Scopes
YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
GSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Authenticate YouTube using OAuth
def get_authenticated_youtube():
    creds = None
    # Check if tokens.json exists (it won't persist in GitHub Actions)
    if os.path.exists("tokens.json"):
        try:
            creds = Credentials.from_authorized_user_file("tokens.json", YOUTUBE_SCOPES)
            print("Loaded credentials from tokens.json")
        except Exception as e:
            print(f"Error loading tokens.json: {e}")

    # If credentials are missing or invalid, create new ones using refresh_token
    if not creds or not creds.valid:
        print("Credentials missing or invalid, attempting to fetch new token...")
        try:
            refresh_token = os.environ.get("REFRESH_TOKEN")
            if not refresh_token:
                raise ValueError("REFRESH_TOKEN environment variable is missing")
            print(f"Using refresh_token: {refresh_token[:10]}...")  # Log first 10 chars for debugging
            
            # Construct credentials using the refresh_token
            creds = Credentials(
                token=None,  # Access token will be fetched using refresh_token
                refresh_token=refresh_token,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=os.environ["CLIENT_ID"],
                client_secret=os.environ["CLIENT_SECRET"],
                scopes=YOUTUBE_SCOPES
            )
            
            # Refresh the credentials to get a new access token
            creds.refresh(Request())
            print("Successfully fetched new access token using refresh_token")
        except Exception as e:
            print(f"Error refreshing token: {e}")
            raise

        # Save the new credentials to tokens.json
        try:
            with open("tokens.json", "w") as token:
                token.write(creds.to_json())
            print("Saved new credentials to tokens.json")
        except Exception as e:
            print(f"Error saving tokens.json: {e}")

    return build("youtube", "v3", credentials=creds)

# Upload video to YouTube Shorts
def upload_video(youtube, video_file, title, description):
    request_body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": description.split(),
            "categoryId": "22"
        },
        "status": {
            "privacyStatus": "public",
            "selfDeclaredMadeForKids": False
        }
    }

    media_file = MediaFileUpload(video_file, chunksize=-1, resumable=True, mimetype="video/*")
    response = youtube.videos().insert(
        part="snippet,status",
        body=request_body,
        media_body=media_file
    ).execute()

    print(f"✅ Uploaded: {response['snippet']['title']} (Video ID: {response['id']})")
    return response['id']

# Read sheet using service account
def load_sheet(sheet_name, worksheet_name):
    gc = gspread.service_account_from_dict({
        "type": "service_account",
        "project_id": os.environ["GCP_PROJECT_ID"],
        "private_key_id": os.environ["GCP_PRIVATE_KEY_ID"],
        "private_key": os.environ["GCP_PRIVATE_KEY"].replace("\\n", "\n"),
        "client_email": os.environ["GCP_CLIENT_EMAIL"],
        "client_id": os.environ["GCP_CLIENT_ID"],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": os.environ["GCP_CLIENT_X509_CERT_URL"]
    })
    sh = gc.open(sheet_name)
    worksheet = sh.worksheet(worksheet_name)
    df = pd.DataFrame(worksheet.get_all_records())
    return df, worksheet

# Write updated data back to sheet
def save_to_sheet(df, worksheet):
    worksheet.clear()
    set_with_dataframe(worksheet, df)

# Download video using requests
def download_video(url, output_file):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(output_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"✅ Downloaded video to {output_file}")
    else:
        raise Exception(f"Failed to download video from {url}. Status code: {response.status_code}")

# Main upload logic
def run_upload():
    try:
        youtube = get_authenticated_youtube()
    except Exception as e:
        print(f"Error authenticating with YouTube: {e}")
        return

    sheet_name = "InstaAuto"
    worksheet_name = "Sheet1"

    try:
        df, worksheet = load_sheet(sheet_name, worksheet_name)
    except Exception as e:
        print(f"Error loading Google Sheet: {e}")
        return

    utc_now = datetime.utcnow()
    ist_offset = 5 * 3600 + 30 * 60
    ist_now = datetime.fromtimestamp(utc_now.timestamp() + ist_offset)
    current_date = ist_now.date()

    # Default to a past date if Upload Time is missing
    default_date = datetime(1970, 1, 1)
    # Check how many videos were posted today
    posted_today = sum(1 for i, row in df.iterrows() if row["Posted"] == "TRUE" and pd.to_datetime(row.get("Upload Time", default_date)).date() == current_date)
    print(f"Number of videos posted today ({current_date}): {posted_today}")

    if posted_today >= 2:
        print("Already posted 2 videos today. Exiting...")
        return

    # Check how many videos are available to upload (case-insensitive and handle None/empty)
    available_to_upload = sum(1 for i, row in df.iterrows() if str(row["Posted"]).strip().upper() != "TRUE")
    print(f"Number of videos available to upload: {available_to_upload}")

    uploaded = False
    for i, row in df.iterrows():
        # Case-insensitive check for "TRUE"
        if str(row["Posted"]).strip().upper() != "TRUE":
            print(f"\n▶️ Uploading: {row['Caption']}")
            video_url = row["Reel URL"]
            video_file = "temp.mp4"

            try:
                download_video(video_url, video_file)
                video_id = upload_video(youtube, video_file, row["Caption"], row["Hashtags"])
                os.remove(video_file)

                df.at[i, "Posted"] = "TRUE"
                df.at[i, "Upload Time"] = ist_now.strftime('%Y-%m-%d %H:%M:%S')
                uploaded = True

                print(f"Video uploaded at {ist_now.strftime('%Y-%m-%d %H:%M:%S')} IST: https://www.youtube.com/shorts/{video_id}")
                # Successfully uploaded, no need to continue
                break
            except Exception as e:
                print(f"Error uploading video: {e}")
                if os.path.exists(video_file):
                    os.remove(video_file)
                # Continue to try the next row
                continue

    if not uploaded:
        print("No more videos to upload. Exiting...")
        return

    try:
        save_to_sheet(df, worksheet)
        print("\n✅ Upload Done!")
    except Exception as e:
        print(f"Error saving to Google Sheet: {e}")

if __name__ == "__main__":
    run_upload()
