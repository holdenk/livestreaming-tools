#!/home/hkarau/repos/livestreaming-tools/myvenv/bin/python
from __future__ import print_function

import datetime
import json
import os
from os.path import expanduser
import pytz
import random
import re
import sets
import sys
from string import Formatter
import time
from tzlocal import get_localzone
import markdown2

import bufferapp
from bs4 import BeautifulSoup
import bitly_api
from dateutil import parser
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from twitch import TwitchClient
from itertools import chain, imap
import memoized
import yaml
import logging

from embed_helpers import *

logging.basicConfig()
logger = logging.getLogger(__name__)

def flatMap(f, items):
    return chain.from_iterable(imap(f, items))


def unix_time_seconds(dt):
    epoch = pytz.UTC.localize(datetime.datetime.utcfromtimestamp(0))
    return (dt - epoch).total_seconds()


# Retrieve a list of the liveStream resources associated with the currently
# authenticated user's channel.
def list_streams(youtube):
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
            parsed_time = parsed_time.replace(tzinfo=pytz.UTC)
            timezone = pytz.timezone('US/Pacific')
            parsed_time = parsed_time.astimezone(timezone)
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
def get_authenticated_google_services():
    # This OAuth 2.0 access scope allows for read-only access to the authenticated
    # user's account, but not other types of account access.
    SCOPES = ['https://www.googleapis.com/auth/youtube.readonly',
              'https://www.googleapis.com/auth/calendar.readonly',
              'https://www.googleapis.com/auth/blogger']
    # Look for the AUTH and client secrets file
    CLIENT_SECRETS_FILE = os.getenv(
        "GOOGLE_CLIENT_SECRET",
        "{0}/g_client_secrets_file".format(expanduser("~")))
    AUTH_FILE = os.getenv(
        "G_AUTH_FILE",
        "{0}/g_auth_file".format(expanduser("~")))
    if not os.path.isfile(AUTH_FILE) and not os.path.isfile(CLIENT_SECRETS_FILE):
        print("Could not find auth file or client secrets. Either place in default location"
              "or set G_AUTH_FILE / CLIENT_SECRETS_FILE to path.")
        sys.exit(-1)

    def yt_cred_to_dict(credentials):
        """Convert the credentials into a form we can serialize."""
        return {
            'token': credentials.token,
            'refresh_token': credentials.refresh_token,
            'id_token': credentials.id_token,
            'token_uri': credentials.token_uri,
            'client_id': credentials.client_id,
            'client_secret': credentials.client_secret,
            'scopes': credentials.scopes,
            'expiry': datetime.datetime.strftime(credentials.expiry, '%Y-%m-%d %H:%M:%S')
        }

    try:
        with open(AUTH_FILE) as data_file:
            logger.debug("Loading credentials")
            credentials_dict = json.load(data_file)
            del credentials_dict['expiry']
            credentials = google.oauth2.credentials.Credentials(**credentials_dict)
            request = google.auth.transport.requests.Request()
            credentials.refresh(request)
            if not credentials.valid:
                logger.error("Credentials aren't valid, trying to refresh...")
                raise Exception("I'm sad, creds aren't happy")
            logger.debug("Using saved credentials")
    except Exception as e:
        logger.debug("Failed to use saved credentials {e}".format(e=e))
        if not os.path.isfile(CLIENT_SECRETS_FILE):
            print("Could not find client secrets file. Either place in default location"
                  "or set  GOOGLE_CLIENT_SECRET to path. Required to auth new flow.")
            sys.exit(-1)

        flow = InstalledAppFlow.from_client_secrets_file(
                                CLIENT_SECRETS_FILE,
                                scopes=SCOPES)
        credentials = flow.run_console()
    with open(AUTH_FILE, 'w') as outfile:
        json.dump(yt_cred_to_dict(credentials), outfile)

    yt_service = build('youtube', 'v3', credentials=credentials)
    cal_service = build('calendar', 'v3', credentials=credentials)
    blog_service = build('blogger', 'v3', credentials=credentials)
    logger.debug("Done authenticating")
    return (yt_service, cal_service, blog_service)


def copy_todays_events(now, events, streams):
    # Filter to streams in the next 7 days
    def soon(stream):
        delta = stream['scheduledStartTime'] - now
        return delta > datetime.timedelta(minutes=5) and \
            delta < datetime.timedelta(days=7)

    upcoming_streams = filter(soon, streams)

    # Filter to events in the next 7 days
    def soon_event(event):
        # We always have a date, we might not know what time were speaking like at DDTX
        delta = event['date'] - now.date()
        if 'start' in event and event['start'] is not None:
            delta = event['start'] - now
        return delta > datetime.timedelta(minutes=5) and \
            delta < datetime.timedelta(days=7)

    upcoming_events = filter(soon_event, events)

    twitch_link = "https://www.twitch.tv/holdenkarau"
    # Update buffer posts
    logger.debug("Updating posts...")
    buffer_clientid = os.getenv("BUFFER_CLIENTID")
    buffer_client_secret = os.getenv("BUFFER_CLIENT_SECRET")
    buffer_token = os.getenv("BUFFER_CODE")

    buffer_api = bufferapp.API(
        client_id=buffer_clientid, client_secret=buffer_client_secret,
        access_token=buffer_token)
    user = bufferapp.User(api=buffer_api)
    profiles = bufferapp.Profiles(api=buffer_api).all()

    # TODO(holden): Import talks from a special calendar
    # TODO(holden): Create a meta post of the weeks events

    def cleanup_event_title(title):
        cleaned_title = title[:1].lower() + title[1:]
        # Cut the text for twitter if needed
        short_title = cleaned_title
        # swap in at mentions on twitter
        short_title = short_title.replace("Apache Spark", "@ApacheSpark") \
            .replace("Apache Airflow (Incubating)", "@ApacheAirflow") \
            .replace("Apache (Incubating) Airflow", "@ApacheAirflow") \
            .replace("Apache Airflow", "@ApacheAirflow") \
            .replace("Apache Beam", "@ApacheBeam") \
            .replace("Kubernetes", "@kubernetesio") \
            .replace("Apache Arrow", "@ApacheArrow")
        short_title = re.sub(" [sS]cala(\.| |\,)", r" @scala_lang\1", short_title)
        short_title = re.sub("^[sS]cala(\.| |\,)", r"@scala_lang\1", short_title)
        short_title = re.sub("[jJ]upyter( |)[cC]on", "@JupyterCon", short_title)
        short_title = re.sub("[sS]trata( |)[cC]onf", "@strataconf", short_title)
        short_title = short_title.replace("@@", "@")
        if len(short_title) > 150:
            short_title = cleaned_title[:150] + "..."
        return (cleaned_title, short_title)

    def format_event_post(event):
        """Create posts for a given event."""
        # TODO(holden): Format the event post
        title, short_title = cleanup_event_title(event['title'])
        city_name = None
        if event['location'] is not None:
            city_name = event['location']
            if "," in city_name:
                city_name = city_name.split(",")[0]

        # Add the tags field with a space so it doesn't join the link
        tag_text = ""
        if 'tags' in event and event['tags']:
            if type(event['tags']) is str:
                tag_text = " / {0}".format(event['tags'])
            else:
                tag_text = " / {0}".format(" ".join(event['tags']))

        # We handle future events & past events differently
        def format_future(format_time_func, delta):
            hey_friends = ""
            if city_name is not None:
                hey_friends = "Hey {0} friends, ".format(city_name)
            # Figure out the join on text
            who = "me"
            if 'copresenters' in event and event['copresenters'] is not None:
                if len(event['copresenters']) == 1:
                    who = "me and {0} ".format(event['copresenters'][0])
                else:
                    who = "{0} and myself".format(", ".join(event['copresenters']))
            join_on = "join {0}".format(who)
            if 'event_name' in event and event['event_name'] is not None:
                event_name = event['event_name']
                # For event names that are twitter handles don't dupe the @
                if "@" in event_name:
                    join_on = "join {0} {1} ".format(who, event['event_name'])
                else:
                    join_on = "join {0} @ {1} ".format(who, event['event_name'])
            # We often have a time, always have a date
            join_at = " @ {0}".format(format_time_func(event))
            link_text = ""
            if event['short_post_link'] is not None:
                link_text = " {0}".format(event["short_post_link"])
            elif event['short_talk_link'] is not None:
                link_text = " {0}".format(event['short_talk_link'])
            
            full_text = "{0}{1}{2} for {3}{4}".format(
                hey_friends, join_on, join_at, title, link_text)
            short_text = "{0}{1}{2} for {3}{4}{5}".format(
                hey_friends, join_on, join_at, short_title, link_text, tag_text)

            # tweet lenght fall back
            if len(short_text) > 250:
                short_string = "{0}{1}{2}{3}".format(
                    join_on, join_at, short_title, link_text)

            return (full_text, short_text, event['start'] - delta,
                    None, event['short_talk_link'], short_title)

        def format_past():
            # TODO(holden): Figure out media links for past talks
            if event['slides_link'] and event['video_link']:
                mini_link = "{short_slides_link} and {short_video_link}"
                if event['short_post_link']:
                    mini_link = "{short_post_link} (or direct {short_slides_link} / {short_video_link})"
                mini_link = mini_link.format(**event)
                full_text = "Slides and video now up from {title} at {mini_link}".format(
                    title=title,
                    mini_link=mini_link)
                short_text = "Slides and video now up from {short_title} at {mini_link}{tag_text}".format(
                    short_title=short_title,
                    mini_link=mini_link,
                    tag_text=tag_text)
                if len(short_text) > 230:
                    if event['short_post_link']:
                        short_text = "Slides & video from {0} at {1}".format(
                            short_title, event['short_post_link'])
                    else:
                        short_text = "Slides & video from {0} at {1} & {2}".format(
                            short_title, event['short_slides_link'], event['short_video_link'])
                return (full_text, short_text, None, None, None, event['short_video_link'], short_title)
            # TODO(holden): Add a function to check if the slides have been linked on video.
            elif event['slides_link']:
                full_text = "Slides now up from {title} at {short_slides_link} :)".format(
                    title=title, short_slides_link=event['short_slides_link'])
                short_text = "Slides now up from {short_title} at {short_slides_link}{tag_text}:)".format(
                    short_title=short_title,
                    short_slides_link=event['short_slides_link'],
                    tag_text=tag_text)
                if len(short_text) > 230:
                    short_text = "Slides from {short_title} @ {short_slides_link}".format(
                        short_title=short_title,
                        short_slides_link=event['short_slides_link'])
                return (full_text, short_text, None, None, None, event['short_slides_link'], short_title)
            else:
                return None

        if (event['start'] is not None and event['start'] > now) or (event['date'] > now.date()):
            # Post for join me today
            def format_time_join_me_today(event):
                has_time = 'start' in event and event['start'] is not None
                if not has_time:
                    return "today"
                time = event['start']
                if time.minute == 0:
                    return time.strftime("today @ %-I%p")
                else:
                    return time.strftime("today @ %-I:%M%p")

            todaydelta = datetime.timedelta(hours=4, minutes=55)
            today_post = format_future(format_time_join_me_today, todaydelta)
            # Skip everything else if we're already at today
            if event['start'].date() == now.date():
                return [today_post]

            # Post for join me this week
            def format_time_join_me_this_week(event):
                has_time = 'start' in event and event['start'] is not None
                if not has_time:
                    return event['date'].strftime("%A")
                time = event['start']
                if time.minute == 0:
                    return time.strftime("%A @ %-I%p")
                else:
                    return time.strftime("%A @ %-I:%M%p")

            thisweekdelta = datetime.timedelta(days=5, minutes=55)
            this_week = format_future(format_time_join_me_this_week, thisweekdelta)

            return [today_post, this_week]
        else:
            past = format_past()
            if past:
                return [past]
            else:
                return []
        

    def format_stream_post(stream):
        """Create posts for a given stream.
        Returns the short text, long text, and  tuple of schedule time."""
        # Munge the text to fit within our sentence structure
        cleaned_title, short_title = cleanup_event_title(stream['title'])
        # Compute how far out this event is
        delta = stream['scheduledStartTime'] - now
        yt_link = stream['url']

        def create_post_func(time_format_func, delta, format_string):
            def create_post(stream):
                tweet_time = stream['scheduledStartTime'] - delta
                media_img = stream['image_url']
                stream_time = time_format_func(stream['scheduledStartTime'])
                coming = ""
                if stream['scheduledStartTime'].isocalendar()[1] != tweet_time.isocalendar()[1]:
                    coming = " coming"

                full_text = format_string.format(
                    stream_time, cleaned_title, yt_link, twitch_link, coming)
                short_text = format_string.format(
                    stream_time, short_title, yt_link, twitch_link, coming)
                return (full_text, short_text, tweet_time, media_img, yt_link,
                        cleaned_title)

            return create_post

        def format_time_same_day(time):
            if time.minute == 0:
                return time.strftime("%-I%p")
            else:
                return time.strftime("%-I:%M%p")

        create_join_in_less_than_an_hour = create_post_func(
            format_time_same_day,
            datetime.timedelta(minutes=random.randrange(39, 55, step=1)),
            "Join me in less than an hour @ {0} pacific for {1} on {2} @YouTube or {3} twitch")

        def format_time_tomorrow(time):
            if time.minute == 0:
                return time.strftime("%a %-I%p")
            else:
                return time.strftime("%a %-I:%M%p")

        create_join_tomorrow = create_post_func(
            format_time_tomorrow,
            datetime.timedelta(hours=23, minutes=55),
            "Join me tomorrow @ {0} pacific for {1} on {2} @YouTube")

        def format_time_future(time):
            if time.minute == 0:
                return time.strftime("%A @ %-I%p")
            else:
                return time.strftime("%A @ %-I:%M%p")

        create_join_me_on_day_x = create_post_func(
            format_time_future,
            datetime.timedelta(
                days=5,
                hours=random.randrange(20, 24, step=1),
                minutes=random.randrange(0, 55, step=1)),
            "Join me this{4} {0} pacific for {1} on {2} @YouTube")

        if stream['scheduledStartTime'].day == now.day:
            # Special case stream on the same day
            return [create_join_in_less_than_an_hour(stream)]
        else:
            # All possible posts leave it up to scheduler
            return [create_join_in_less_than_an_hour(stream),
                    create_join_me_on_day_x(stream),
                    create_join_tomorrow(stream)]

    possible_stream_posts = list(flatMap(format_stream_post, upcoming_streams))
    possible_event_posts = list(flatMap(format_event_post, events))

    possible_posts = []
    possible_posts.extend(possible_stream_posts)
    possible_posts.extend(possible_event_posts)

    # Only schedule posts in < 36 hours and < - 12 hours
    def is_reasonable_time(post):
        # If we don't have a time to schedule always a good time
        if post[2] is None:
            return True
        delta_from_now = post[2] - now
        return delta_from_now < datetime.timedelta(hours=25, minutes=55) and \
            delta_from_now > datetime.timedelta(days=-5)

    desired_posts = filter(is_reasonable_time, possible_posts)

    def post_as_needed_to_profile(profile):
        # Special case twitter for short text
        posts = []
        logger.debug(profile.formatted_service)
        if profile.formatted_service == u"Twitter":
            posts = map(lambda post: (post[1], post[2], post[3], post[4], post[5]),
                        desired_posts)
        else:
            posts = map(lambda post: (post[0], post[2], post[3], post[4], post[5]),
                        desired_posts)
        updates = profile.updates
        pending = updates.pending
        sent = updates.sent
        all_updates = []
        all_updates.extend(pending)
        all_updates.extend(sent)

        # Get the raw text of the posts to de-duplicate
        def extract_text_from_update(update):
            return unicode(BeautifulSoup(
                update.text_formatted,
                features="html.parser").get_text())

        # Allow comparison after munging
        def clean_odd_text(text):
            text = re.sub("http(s|)://[^\s]+", "", text)
            return mini_clean_text(text)

        def mini_clean_text(text):
            # media tag
            text = text.replace(u"\xa0\xa0", "")
            # Spaces get screwy :(
            text = text.replace(" ", "")
            # And +s...
            text = text.replace("+", "")
            # Something something &nbsp;
            text = text.replace("\t", "")
            text = text.replace("&nbsp;", "")
            text = text.replace('["', "")
            text = text.replace(']', "")
            return unicode(text.lower())

        # Get the text and link
        def extract_special(update):
            text = BeautifulSoup(
                update.text_formatted,
                features="html.parser").get_text()
            text = clean_odd_text(text)
            media_link = None
            if hasattr(update, "media"):
                media_link = update.media.get("link", None)
            return (unicode(text), unicode(media_link))

        all_updates_text = sets.Set(map(extract_text_from_update, all_updates))
        all_updates_partial_text = sets.Set(
            map(mini_clean_text, map(extract_text_from_update, all_updates)))
        # Kind of a hack cause of how media links is handled
        all_updates_special = sets.Set(map(extract_special, all_updates))

        def allready_published(post):
            in_all_updates_text = unicode(post[0]) in all_updates_text
            in_partial_text = unicode(mini_clean_text(post[0])) in all_updates_partial_text
            logger.debug("Special: {0}".format((unicode(clean_odd_text(post[0])), unicode(post[3]))))
            in_special = (unicode(clean_odd_text(post[0])), unicode(post[3])) in all_updates_special
            in_special_ish = (unicode(clean_odd_text(post[0])), unicode(post[4])) in all_updates_special
            return in_all_updates_text or in_partial_text or in_special or in_special_ish

        unpublished_posts = filter(
            lambda post: not allready_published(post), posts)

        logger.debug("Prepairing to update with new posts:")
        logger.debug(unpublished_posts)

        updates = profile.updates
        for post in unpublished_posts:
            # Note: even though we set shorten the backend seems to use the
            # user's per-profile settings instead.
            media = None
            if post[2] is not None:
                        media = {"thumbnail": post[2], "link": post[3], "picture": post[2],
                                 "description": post[4]}
            try:
                if post[1] is None:
                    updates.new(post[0], shorten=False)
                elif post[1] > now:
                    target_time_in_utc = post[1].astimezone(pytz.UTC)
                    updates.new(post[0], shorten=False, media=media,
                                when=unix_time_seconds(target_time_in_utc))
                else:
                    updates.new(post[0], shorten=False,
                                now=True)
            except Exception as e:
                logger.warn("Skipping update {0}".format(e))
                logger.warn(post)

    for profile in profiles:
        post_as_needed_to_profile(profile)

    def update_twitch():
        """Update twitch. Broken until client lib switches to new API."""
        # Set up twitch posts
        twitch_client = TwitchClient(
            client_id=os.getenv("TWITCH_CLIENT_ID"),
            oauth_token=os.getenv("TWITCH_OAUTH"))
        channel_info = twitch_client.channels.get()
        channel_id = channel_info.id
        logger.debug(channel_id)
        # Get existing updates
        posts = twitch_client.channel_feed.get_posts(
            channel_id=channel_id, comments=None)
        # Ugh this is deprecated now
        # TODO: Wait for twitch client to update to Helix API


def update_stream_header(now, streams):
    """Update review_info.txt to the next scheduled stream."""
    todays_streams = list(
        filter(lambda stream: stream['scheduledStartTime'].date() == now.date(), streams))

    def write_header_for_stream(stream):
        logger.debug("Updating header for stream {0}".format(stream))
        review_header_name = "{0}/review_info.txt".format(expanduser("~"))
        with open(review_header_name, 'w') as f:
            f.write(stream['title'])

    if len(todays_streams) == 0:
        return
    elif len(todays_streams) == 1:
        write_header_for_stream(todays_streams[0])
    else:
        def stream_is_soon(stream):
            delta = stream['scheduledStartTime'] - now
            return delta < datetime.timedelta(minutes=30) and delta > datetime.timedelta(minutes=-5)

        possible_stream = list(filter(stream_is_soon, stream))
        if possible_stream is None:
            return
        else:
            write_header_for_stream(possible_stream[0])


def get_streams(yt_service):
    """Fetch upcoming youtube streams."""
    # Fetch youtube streams
    logger.debug("Fetching YouTube streams...")
    streams = list_streams(yt_service)
    # Get a noew in pacific time we can use for scheduling and testing
    # Assumes system time is in pacific or UTC , which holds true on my home computer :p
    return streams


@memoized.memoized
def shortten(link):
    """Shortten a link if it is provided."""
    if link is None:
        return None
    token = os.getenv("BITLY_TOKEN")
    bitly = bitly_api.Connection(access_token=token)
    data = bitly.shorten(link)
    return str(data['url'])


def process_event_yaml(yaml_txt):
    """Process the event YAML"""
    parsed_description = dict(yaml.load(yaml_txt) or {})
    return annotate_parsed_events(parsed_description)

def annotate_parsed_events(parsed):
    link_keys = [
        "talk_link", "slides_link", "video_link", "event_link", "post_link",
        "repo_link", "discussion_link"]
    short_link_keys = map(lambda x: "short_" + x, link_keys)
    raw_keys = ["start", "location", "title", "description", "parsed", "post_id"]
    string_keys = ["location", "title", "event_name", "talk_description",
                   "last_post_text", "blog_fmt_text", "event_type"]
    time_keys = ["date", "start", "synced_to_blog"]
    listish_keys = ["copresenters", "tags"]
    relevant_keys = raw_keys + link_keys + short_link_keys + listish_keys + time_keys + string_keys
    result = dict(map(
        lambda key: (key, parsed.get(key, None)),
        relevant_keys))
    # Process the links
    def process_link(keyname):
        if result[keyname] is not None and result["short_" + keyname] is None:
            result["short_" + keyname] = shortten(result[keyname])

    map(process_link, link_keys)

    # Process the times
    def update_time(keyname):
        if type(result[keyname]) is str:
            result[keyname] = parser.parse(result[keyname])

    map(update_time, time_keys)

    # Handle quazi list keys
    def quazi_list_keys(keyname):
        if type(result[keyname]) is str:
            result[keyname] = [result[keyname]]

    map(quazi_list_keys, listish_keys)

    # Handle the stringy keys
    def handle_string_ish_key(keyname):
        if result[keyname] is not None:
            # TODO: handle unicode input
            result[keyname] = result[keyname].encode('ascii', 'ignore')
    map(handle_string_ish_key, string_keys)

    # Warn if we have unexpected keys
    unexpected = {key:value for key, value in parsed.items() if key not in relevant_keys}
    if len(unexpected) > 0:
        logger.warn("Unexpected keys {0} from {1}".format(unexpected, parsed))

    return dict(result)

def pre_annotate_event(event):
    if event["date"] is None and event["start"] is not None:
        event["date"] = event["start"].date()
    return event

def get_file_events(events_input_filename):
    """Fetch events from file"""
    with open(events_input_filename) as yaml_stream:
        loaded_yaml = yaml.load(yaml_stream)
        def process_event(k_v):
            key, value = k_v
            result = annotate_parsed_events(value)
            if result["event_name"] is None:
                result["event_name"] = key
            return result

        return map(process_event, loaded_yaml.items())

def get_cal_events(cal_service):
    """Fetch calendar events"""
    # Todo(later): unhardcode this if other folks want to use it
    calendarId = "dqauku3a2tjqj7hc1psgnaeshs@group.calendar.google.com"
    # Subtract 6 months
    start_time = datetime.datetime.utcnow() - datetime.timedelta(days=6*30)
    formatted_min_time = start_time.isoformat() + 'Z' # 'Z' indicates UTC time

    events_result = cal_service.events().list(
        calendarId=calendarId, timeMin=formatted_min_time,
        maxResults=75, singleEvents=True,
        orderBy='startTime').execute()
    def post_process_event(cal_event):
        """Extract useful fields from the event."""
        parsed_time = parser.parse(str(cal_event['start']['dateTime']))
        if 'timeZone' in cal_event['start']:
            timezone = pytz.timezone(cal_event['start']['timeZone'])
            parsed_time = parsed_time.astimezone(timezone)
        description_text = cal_event.get('description', None) or ""
        result = process_event_yaml(description_text)
        # Augment result with the time info
        result["start"] = parsed_time
        if not result["title"]:
            result["title"] = str(cal_event['summary'])
        if not result["location"] and 'location' in cal_event:
            result["location"] = str(cal_event['location'])
        return result

    events = events_result.get('items', [])
    return map(post_process_event, events)

def make_event_blogs(events, blog_service):
    """Make the posts for the provided events.
    Mutates the events to contain the new post text if we generate a post."""
    event_and_posts = map(lambda event: (event, format_event_blog(event)), events)
    event_and_posts_to_be_updated = filter(
        lambda e_p: e_p[0]["post_id"] is not None and e_p[0]["last_post_text"] != e_p[1],
        event_and_posts)
    event_and_posts_to_be_created = filter(
        lambda e_p: e_p[0]["last_post_text"] is None and e_p[0]["post_id"] is None,
        event_and_posts)
    logger.debug(dir(blog_service))
    logger.debug(dir(blog_service.blogs()))
    blog_id_query = blog_service.blogs().getByUrl(url="http://blog.holdenkarau.com")
    blog_id = blog_id_query.execute()['id']
    logger.debug("Blog id {blog_id}".format(blog_id=blog_id))
    for event, post in event_and_posts_to_be_created:
        post_query = blog_service.posts().insert(
            body={"title": event["title"] + " @ " + event["event_name"], "content": post},
            blogId=blog_id)
        post_result = post_query.execute()
        event["post_link"] = str(post_result["url"])
        event["post_id"] = str(post_result["id"])
        event["last_post_text"] = post
        event["short_post_link"] = shortten(event["post_link"])
        # Temporary hack only make one post per call, leave the rest for later
        # so as to not overwhelm. TODO(holden) -- better schedualing
        break
    for event, post in event_and_posts_to_be_updated:
        post_query = blog_service.posts().update(
            body={"title": event["title"] + " @ " + event["event_name"], "content": post},
            blogId=blog_id, postId=event["post_id"])
        post_query.execute()
        event["last_post_text"] = post
    return events


def tw_link(username):
    if len(username) > 2 and username[0] == "@":
        return ("<a href='https://www.twitter.com/{short_username}'>{username}</a>"
                .format(username=username, short_username=username[1:]))
    else:
        return username


def format_event_blog(event):
    def me_or_us():
        if event["copresenters"] is not None:
            presenters = ["@holdenkarau"]
            presenters.extend(event["copresenters"])
            presenters_html = ",".join(map(tw_link, presenters))
            return "us ({presenters_html})".format(presenters_html=presenters_html)
        else:
            return "<a href='http://www.twitter.com/holdenkarau'>me</a>"

    def thanks_or_come_join():
        if event["date"] < now.date():
            return "Thanks for joining {me_or_us} on {date}"
        else:
            return "Come join {me_or_us} on {date}"

    def year():
        year = str(event["date"].year)
        if year in event["event_name"]:
            return ""
        else:
            return " " + year

    def where():
        if event["event_name"] is not None:
            if event["location"] is not None:
                return "at {event_name}{year} {location}"
        return ""

    def talk_details():
        if event["talk_description"] is not None:
            new_description = markdown2.markdown(event["talk_description"])
            return "The talk covered: {new_description}.".format(new_description=new_description)
        return ""

    def event_type():
        if event["event_type"] is not None:
            return event["event_type"]
        elif "book" in event["title"].lower():
            return "signing"
        else:
            return "talk"

    def talk_links():
        link_text = ""
        if event["short_repo_link"] is not None:
            link_text += 'You can find the code for this <a href="{short_repo_link}">talk at {repo_link}</a>.'
        if event["short_slides_link"] is not None:
            link_text += 'The <a href="{short_slides_link}">slides are at {short_slides_link}</a>.'
        if event["short_video_link"] is not None:
            link_text += 'The <a href="{short_video_link}">video of the talk is up at {short_video_link}</a>.'
        # Put the link's in a paragraph.
        if link_text != "":
            link_text = "<p>{0}</p>".format(link_text)
        if link_text == "" and event_type() == "talk":
            link_text = "I'll update this post with the slides soon."
        return link_text

    def talk_embeds():
        embed_text = ""
        if is_youtube(event["video_link"]):
            embed_text += embed_youtube(event["video_link"])
        elif is_vimeo(event["video_link"]):
            embed_text += embed_vimeo(event["video_link"])
        if is_slideshare(event["slides_link"]):
            embed_text += embed_slideshare(event["slides_link"])
        return embed_text

    def discussion():
        if event["discussion_link"]:
            return '<a href="{short_discussion_link}">Join in the discussion at {short_discussion_link}</a> :)'
        elif event['date'] < now.date():
            return "Comment bellow to join in the discussion :)"
        else:
            return "Come see to the {event_type} or comment bellow to join in the discussion :)"


    def footer():
        return os.getenv(
            "POST_FOOTER",
            '<a href="http://bit.ly/holdenTalkFeedback">Talk feedback is appreciated at http://bit.ly/holdenTalkFeedback</a>')

    def title_w_link():
        if event['short_talk_link']:
            return '<a href="{short_talk_link}">{title}</a>'
        return '{title}'

    fmt_elements = event.copy()
    other_elements = {
        'event_type': event_type(),
        'me_or_us': me_or_us(), 'year': year(),
        'thanks_or_come_join': thanks_or_come_join(), 'where': where(),
        'talk_details': talk_details(), 'talk_links': talk_links(), 'talk_embeds': talk_embeds(),
        'title_w_link': title_w_link(),
        'discussion': discussion(), 'footer': footer()}
    fmt_elements.update(other_elements)

    # Format until we're done
    c = 0
    post_string = event['blog_fmt_text'] or \
        ("{thanks_or_come_join} {where} for {title_w_link}.{talk_details}{talk_links}"
         "{talk_embeds}{discussion}.{footer}")
    result = post_string.format(**fmt_elements)
    while result != result.format(**fmt_elements):
        result = result.format(**fmt_elements)
    logger.debug(result)
    return result


def load_events():
    events_input_filename = os.getenv(
        "EVENTS_FILE",
        "{0}/repos/talk-info/events.yaml".format(expanduser("~")))
    events = get_cal_events(cal_service)
    events.extend(get_file_events(events_input_filename))
    # Filter out events without minimal requires keys
    def is_valid_event(event):
        required_keys = ["event_name", "title"]
        valid_event = all(key in event and event[key] is not None
                          for key in required_keys)
        if not valid_event:
            logger.debug("Removed event {0}".format(event))
        return valid_event

    valid_events = filter(is_valid_event, events)
    pre_processed_events = map(pre_annotate_event, valid_events)
    # De duplicate events by day and name
    events_dict = {}
    for event in pre_processed_events:
        day = event['date']
        title = event['title']
        event_name = event['event_name']
        key = (day, title, event_name)
        if key not in events_dict:
            events_dict[key] = event
        else:
            # Duplicate! Merge event time. Existing event has priority because *shrug*
            # TODO(holden): Add an updated field maybe and use that for priority?
            existing_event = events_dict[key]
            merged_event_keys = sets.Set(event.keys() + existing_event.keys())
            for key in merged_event_keys:
                if key not in existing_event or existing_event[key] is None:
                    existing_event[key] = event[key]
                
                    
    return events_dict.values()


if __name__ == '__main__':
    logger.setLevel("DEBUG")
    required_envs = ["BUFFER_CLIENTID", "BUFFER_CLIENT_SECRET", "BUFFER_CODE", "BITLY_TOKEN"]

    def check_env_is_set(env_name):
        if os.getenv(env_name) is None:
            logger.error("You must set enviroment variable {0}".format(env_name))
            sys.exit(-1)

    for env in required_envs:
        check_env_is_set(env)

    yt_service, cal_service, blog_service = get_authenticated_google_services()
    streams = get_streams(yt_service)
    now = datetime.datetime.now()

    # Try and work on both my computer and my server. Timezones :(
    timezone = pytz.timezone('US/Pacific')
    local_timezone = get_localzone()
    now = local_timezone.localize(now)
    now = now.astimezone(timezone)

    #update_stream_header(now, streams)
    logger.debug("Fetching events.")
    events = load_events()
    # Make posts for events
    make_event_blogs(events, blog_service)
    events_output_filename = os.getenv(
        "EVENTS_OUT_FILE",
        "{0}/repos/talk-info/events.yaml".format(expanduser("~")))
    with open(events_output_filename, 'w') as f:
        keyed_events = dict(
            map(lambda event: (event["event_name"] + ":" + event["title"], event),
                events))
        yaml.dump(keyed_events, f)
    copy_todays_events(now, events, streams)
