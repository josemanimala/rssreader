def get_feed(url):

    import feedparser

    feed = feedparser.parse(url)

    if feed.bozo != 1:
        return feed
    else:
        raise feed.bozo_exception
