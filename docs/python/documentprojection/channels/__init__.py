from ..utils.reflection import get_subclasses
from ..framework import Channel

default_channels = get_subclasses(__name__, Channel)
