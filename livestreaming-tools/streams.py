import datetime
from utils import time_from_utc_to_pacific

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
            parsed_time = time_from_utc_to_pacific(parsed_time)
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
