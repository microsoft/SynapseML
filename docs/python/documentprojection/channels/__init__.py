from utils.reflection import get_subclasses
from framework import Channel

all_channels = get_subclasses(__name__, Channel)
