from .utils.logging import *
from .utils.notebook import *
from .framework.pipeline import *
from .channels.website import *

import re

log = get_log(__name__)


def get_channel_map():
    def camel_to_snake(name):
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
        return s2.replace("_channel", "")

    channel_map = {
        k: v
        for k, v in [
            (camel_to_snake(channel.__name__), channel) for channel in all_channels
        ]
    }
    return channel_map


def parse_args():
    channel_choice_names = ["all"] + list(get_channel_map())
    log_level_choices = ["debug", "info", "warn", "error", "critical"]

    import argparse

    parser = argparse.ArgumentParser(description="Document Projection Pipeline")
    parser.add_argument(
        "notebooks",
        metavar="N",
        type=str,
        nargs="*",
        help="a notebook or folder of notebooks to project",
    )
    parser.add_argument(
        "-f", "--format", action="store_true", default=False, help="run only formatters"
    )
    parser.add_argument(
        "-p",
        "--publish",
        action="store_true",
        default=False,
        help="run publishers. forces -t and -f.",
    )
    parser.add_argument(
        "-m",
        "--metadata",
        action="store_true",
        default=False,
        help="format documents and only output metadata of formated documents.",
    )
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        default=False,
        help="recursively scan for notebooks in the given directory.",
    )
    parser.add_argument(
        "-l", "--list", action="store_true", default=False, help="list notebooks"
    )
    parser.add_argument(
        "-c",
        "--channel",
        choices=channel_choice_names,
        default="console",
        help="the channel through which the notebook(s) should be processed. defaults to all if not specified.",
    )
    parser.add_argument(
        "-v",
        "--loglevel",
        choices=log_level_choices,
        default="info",
        help="set log level",
    )
    return parser.parse_args()


def run():
    args = parse_args()
    config_log(args.loglevel)
    log.debug("script executed with args: {}".format(args))

    if len(args.notebooks) == 0:
        args.notebooks = [get_mock_path()]

    notebooks = collect_notebooks(args.notebooks, args.recursive)
    if args.list:
        log.info("Notebooks found:\n{}".format("\n".join(map(repr, notebooks))))
        log.info("--info specified. Will not process any notebooks.")
        return
    log.debug("notebooks specified: {}".format(notebooks))

    pipeline = DocumentProjectionPipeline(config=PipelineConfig(vars(args)))
    channels = (
        all_channels if args.channel == "all" else [get_channel_map()[args.channel]]
    )
    pipeline.register_channels(channels)
    pipeline.run(notebooks)


run()
