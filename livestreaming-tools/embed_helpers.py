import oembed

consumer = oembed.OEmbedConsumer()
slideshare_endpoint = oembed.OEmbedEndpoint('http://www.slideshare.net/api/oembed/2', ['http*://*.slideshare.net/*'])
consumer.addEndpoint(slideshare_endpoint)

def is_youtube(videolink):
    return videolink is not None and \
        ("youtube.com" in videolink or "youtu.be" in videolink)


def embed_youtube(videolink):
    """Embed a youtube video. I hope."""
    url = videolink
    url = url.replace("https://youtu.be/", "https://www.youtube.com/embed/")
    url = url.replace("watch?v=", "embed/")
    return '<iframe width="560" height="315" src="{url}" frameborder="0" allow="autoplay; encrypted-media" allowfullscreen></iframe>'.format(url=url)


def is_slideshare(slidelink):
    return slidelink is not None and "slideshare.net" in slidelink

def is_vimeo(videolink):
    # TODO: handle vimeo
    return False

def embed_slideshare(slidelink):
    return consumer.embed(slidelink).getData()['html']
