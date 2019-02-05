import datetime

import memoized
import pytz
from tzlocal import get_localzone
from HTMLParser import HTMLParser

def get_now_to_match(date):
    if date is datetime.date:
        return datetime.datetime.now()
    else:
        return pacific_now()

@memoized.memoized
def pacific_now():
    now = datetime.datetime.now()

    # Try and work on both my computer and my server. Timezones :(
    timezone = pytz.timezone('US/Pacific')
    local_timezone = get_localzone()
    now = local_timezone.localize(now)
    now = now.astimezone(timezone)
    return now

def time_from_utc_to_pacific(input_time):
    utc_time = input_time.replace(tzinfo=pytz.UTC)
    pacific_timezone = pytz.timezone('US/Pacific')
    pacific_time = utc_time.astimezone(pacific_timezone)
    return pacific_time

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)
