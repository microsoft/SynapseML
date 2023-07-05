from typing import List
from ..utils.logging import get_log
from .objects import *
from ..utils.notebook import *
from ..utils.parallelism import process_in_parallel

log = get_log(__name__)


class PipelineConfig(dict):
    def __init__(self, dict: dict):
        self.__dict__.update(dict)

    format = None
    publish = None
    channel = None
    project_root = None
    manifest = None


class DocumentProjectionPipeline:
    def __init__(self, channel_map: dict, config: PipelineConfig = PipelineConfig({})):
        self.channel_map = channel_map
        self.config = config

    def run(self) -> None:
        log.debug(
            f"""DocumentProjectionPipeline running with:
            Mode: {self.config},
            Config: {self.config}"""
        )

        channels = self.config.manifest["channels"]

        if len(channels) == 0:
            raise Exception("No channels registered.")

        if not self.config.publish:
            log.warn(f"PUBLISH mode not enabled. Skipping publish step.")

        for channel_config in channels:
            if channel_config["name"] not in self.channel_map:
                raise Exception(
                    f"Channel declared in manifest but no implementation was found: {channel_config['name']}. If this is a custom channel, make sure you have specified the custom channels folder."
                )

        for channel_config in channels:
            if channel_config["is_active"] == False:
                log.info(
                    f"Skipping channel marked as inactive: {channel_config['name']}"
                )
                continue
            channel_metadata = ChannelMetadata(
                {
                    key: self.config.__dict__[key]
                    for key in ["project_root"]
                    if key in self.config.__dict__
                }
            )
            if "metadata" in channel_config:
                channel_metadata.update(channel_config["metadata"])
            channel = self.channel_map[channel_config["name"]](channel_metadata)

            notebook_metadata = channel_config["notebooks"]
            notebooks = []
            for entry in notebook_metadata:
                parsed_notebooks = parse_notebooks([entry["path"]], recursive=True)
                notebooks.extend(
                    [
                        Notebook(parsed_notebook, metadata=entry)
                        for parsed_notebook in parsed_notebooks
                    ]
                )
            log.info(
                f"Processing {len(notebooks)} notebooks in parallel for: {repr(channel)}"
            )

            formatted_documents = process_in_parallel(channel.format, notebooks)
            if self.config.publish:
                process_in_parallel(channel.publish, formatted_documents)
            if self.config.format:
                for i in range(len(notebooks)):
                    log.info(
                        "Formatted content for {}:\n{}".format(
                            notebooks[i], formatted_documents[i].content
                        )
                    )
                    log.info(f"End formatted content for {notebooks[i]}")


def collect_notebooks(paths: List[str], recursive: bool) -> List[Notebook]:
    return [Notebook(nb) for nb in parse_notebooks(paths, recursive)]
