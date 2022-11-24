import os
import pathlib


def topic_exists(topic: str) -> bool:
    here = pathlib.Path(__file__).parent.resolve()

    if os.path.isdir(f"{here}\\topics\\{topic}"):  # dir_path+topic
        return True
    return False
