#!/home/hkarau/repos/livestreaming-tools/myvenv/bin/python
from __future__ import print_function

import logging
import os

import markdown2

from embed_helpers import *
from shortten import shortten
from utils import pacific_now

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


def tw_link(username):
    if len(username) > 2 and username[0] == "@":
        return ("<a href='https://www.twitter.com/{short_username}'>{username}</a>"
                .format(username=username, short_username=username[1:]))
    else:
        return username


def format_event_blog(event):
    logger.debug("Formatting event title: {0}\n".format(event["title"]))

    now = pacific_now()
    event_in_past = event["date"] < now.date() or \
        ("start" in event and event["start"] and event["start"] < now)

    def me_or_us():
        if event["copresenters"] is not None:
            presenters = ["@holdenkarau"]
            presenters.extend(event["copresenters"])
            presenters_html = ",".join(map(tw_link, presenters))
            return "us ({presenters_html})".format(presenters_html=presenters_html)
        else:
            return "<a href='http://www.twitter.com/holdenkarau'>me</a>"

    def thanks_or_come_join():
        if event_in_past:
            return "Thanks for joining {me_or_us} on {date}"
        else:
            return "Come join {me_or_us} on {time_or_date}"

    def time_or_date():
        if event["start"]:
            return event["start"].strftime("%A %d %B @ %H:%M")
        else:
            return event["date"].strftime("%A %d. %B %Y")

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
        elif event["room"] is not None:
            return "The room will be <b>{room}</b>."
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
        if event["short_codelab_link"] is not None:
            link_text += 'And if you want there is a <a href="{short_codelab_link}">related codelab you can try out</a>.'
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
        elif event_in_past:
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
        'time_or_date': time_or_date(),
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
    return result


def make_event_blogs(events, blog_service):
    """Make the posts for the provided events.
    Mutates the events to contain the new post text if we generate a post."""
    logger.debug("Looking at events {0}".format(events))
    event_and_posts = map(lambda event: (event, format_event_blog(event)), events)
    event_and_posts_to_be_updated = filter(
        lambda e_p: e_p[0]["post_id"] is not None and e_p[0]["last_post_text"] != e_p[1],
        event_and_posts)
    event_and_posts_to_be_created = filter(
        lambda e_p: e_p[0]["last_post_text"] is None and e_p[0]["post_id"] is None,
        event_and_posts)
    logger.debug("New posts to be created {0}".format(event_and_posts_to_be_created))
    logger.debug(dir(blog_service))
    logger.debug(dir(blog_service.blogs()))
    blog_id_query = blog_service.blogs().getByUrl(url="http://blog.holdenkarau.com")
    blog_id = blog_id_query.execute()['id']
    logger.debug("Blog id {blog_id}".format(blog_id=blog_id))
    for event, post in event_and_posts_to_be_created:
        event["changed"] = True
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
        event["changed"] = True
        post_query = blog_service.posts().update(
            body={"title": event["title"] + " @ " + event["event_name"], "content": post},
            blogId=blog_id, postId=event["post_id"])
        post_query.execute()
        event["last_post_text"] = post
    return events
