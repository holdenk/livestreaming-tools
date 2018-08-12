import datetime
import json
import os
import re
import pytz

import bufferapp
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from twitch import TwitchClient

# More or less https://github.com/youtube/api-samples/blob/master/python/list_streams.py

# Retrieve a list of the liveStream resources associated with the currently
# authenticated user's channel.
def list_streams(youtube):
  print 'Live streams:'

  list_streams_request = youtube.liveBroadcasts().list(
    part='id,snippet',
    mine=True,
    maxResults=50
  )

  results = []

  # Collect the results over multiple pages of youtube responses
  while list_streams_request:
    list_streams_response = list_streams_request.execute()

    def extract_information(stream):
        parsed_time = datetime.datetime.strptime(
            str(stream['snippet']['scheduledStartTime']),
            '%Y-%m-%dT%H:%M:%S.000Z')
        parsed_time.replace(tzinfo=pytz.UTC)
        timezone = pytz.timezone('US/Pacific')
        parsed_time = timezone.localize(parsed_time)
        return {
            "title": stream['snippet']['title'],
            "description": stream['snippet']['description'],
            "id": stream['id'],
            "url": "https://www.youtube.com/watch?v={0}".format(stream['id']),
            "scheduledStartTime": parsed_time,
            "image_url": stream['snippet']['thumbnails']['medium']['url']}
                
    responses = list_streams_response.get('items', [])
    future_streams = filter(
        lambda response: "actualEndTime" not in response["snippet"], responses)
    extracted_values = map(extract_information, future_streams)
    results.extend(extracted_values)
    
    list_streams_request = youtube.liveStreams().list_next(
      list_streams_request, list_streams_response)

  return results


# Authorize the request and store authorization credentials.
def get_authenticated_youtube_service():
    # This OAuth 2.0 access scope allows for read-only access to the authenticated
    # user's account, but not other types of account access.
    SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']
    API_SERVICE_NAME = 'youtube'
    API_VERSION = 'v3'
    CLIENT_SECRETS_FILE = os.getenv("GOOGLE_CLIENT_SECRET")
    AUTH_FILE = os.getenv("G_AUTH_FILE", "g_yt_auth_file")

    def yt_cred_to_dict(credentials):
        """Convert the credentials into a form we can serialize."""
        return  {
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'id_token':credentials.id_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes,
        'expiry':datetime.datetime.strftime(credentials.expiry,'%Y-%m-%d %H:%M:%S')
    }

    try:
        with open(AUTH_FILE) as data_file:    
            credentials_dict = json.load(data_file)
            del credentials_dict['expiry']
            credentials = google.oauth2.credentials.Credentials(**credentials_dict)
            request = google.auth.transport.requests.Request()
            credentials.refresh(request)
            if not credentials.valid:
                raise Exception("I'm sad, creds aren't happy")
    except:
        flow = InstalledAppFlow.from_client_secrets_file(
                                CLIENT_SECRETS_FILE,
                                scopes=SCOPES,
                                access_type='offline')
        credentials = flow.run_console()
    with open(AUTH_FILE, 'w') as outfile:
        json.dump(yt_cred_to_dict(credentials), outfile)
    
    return build(API_SERVICE_NAME, API_VERSION, credentials = credentials)


def copy_todays_events():
    # Fetch youtube streams
    #youtube = get_authenticated_youtube_service()
    #streams = list_streams(youtube)
    #for stream in streams:
    #    print(stream)

    # Update buffer posts
    buffer_clientid = os.getenv("BUFFER_CLIENTID")
    buffer_client_secret = os.getenv("BUFFER_CLIENT_SECRET")
    buffer_token = os.getenv("BUFFER_CODE")
    
    buffer_api = bufferapp.API(
        client_id=buffer_clientid, client_secret=buffer_client_secret,
        access_token=buffer_token)
    user = bufferapp.User(api=buffer_api)
    profiles = bufferapp.Profiles(api=api)
    def post_as_needed_to_profile(profile):
        print profile
        print profile.schedules
        # TODO: Filter out the videos which are covered then make posts for the ones which aren't



    # Set up twitch posts
    twitch_client = TwitchClient(
        client_id=os.getenv("TWITCH_CLIENT_ID"),
        oauth_token=os.getenv("TWITCH_OAUTH"))
    channel_info = twitch_client.channels.get()
    channel_id = channel_info.id
    print channel_id
    # Get existing updates
    posts = twitch_client.channel_feed.get_posts(channel_id = channel_id, comments= None)
    # Ugh this is deprecated now
    # TODO: Wait for twitch client to update to Helix API


  
    
if __name__ == '__main__':
    copy_todays_events()
