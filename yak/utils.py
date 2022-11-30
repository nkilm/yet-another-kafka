import os
import pathlib


here = pathlib.Path(__file__).parent.resolve()


def topic_exists(topic: str) -> bool:

    if os.path.isdir(f"{here}\\topics\\{topic}"):  # dir_path+topic
        return True
    return False


def update_metadata(port: int) -> None:
    with open(f"{here}/metadata.txt", "w+") as f:
        f.write(str(port) + "\n")


def read_metadata() -> int:
    with open(f"{here}/metadata.txt", "r") as f:
        leader_port = f.readline().strip()

    return leader_port
