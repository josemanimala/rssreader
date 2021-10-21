import pybson


def to_bson(feed):
    return pybson.dumps(feed)


def from_bson(encoded_feed):
    return pybson.loads(encoded_feed)
