from __future__ import print_function

import os
import bitly_api
import memoized

@memoized.memoized
def shortten(link):
    """Shortten a link if it is provided."""
    if link is None:
        return None
    token = os.getenv("BITLY_TOKEN")
    bitly = bitly_api.Connection(access_token=token)
    try:
        data = bitly.shorten(link)
        return str(data['url'])
    # If we have a pre-shortened link support that
    except bitly_api.bitly_api.BitlyError as e:
        if "ALREADY_A_BITLY_LINK" in str(e):
            return link
        else:
            raise e
    except Exception as e:
        print("Unexepected error e {0} for {1}".format(e, link))
        raise e
