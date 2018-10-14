import datetime
import pytz
import memoized
from tzlocal import get_localzone

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
    
