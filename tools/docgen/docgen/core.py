import multiprocessing
from abc import ABC, abstractmethod
from typing import List


class Channel(ABC):
    @abstractmethod
    def process(self, input_file: str) -> ():
        pass

    @abstractmethod
    def list_input_files(self) -> List[str]:
        pass

    def run(self) -> ():
        for index, input_file in enumerate(self.list_input_files()):
            self.process(input_file, index)


class ParallelChannel(Channel):
    def run(self) -> ():
        with multiprocessing.Pool() as pool:
            pool.map(self.process, self.list_input_files())


class DocumentProcessor:
    def __init__(self, channels: List[Channel]):
        self.channels = channels

    def run(self) -> None:
        print(f"Running DocumentProcessor on {self.channels}")
        if len(self.channels) == 0:
            raise ValueError("No channels selected.")

        for channel in self.channels:
            print(f"Running Channel: {self.channels}")
            channel.run()
