#!/usr/bin/env python3

"""
Functions of zookeeper
- keep track of the leader - Make contacts regularly with the leader
- Elect leader from available brokers
- always running

"""
import subprocess

import requests
from requests.exceptions import ConnectionError
from utils import update_metadata

ONLINE_PORTS: list = sorted([6060, 7070, 8080])
LEADER_PORT: int = ONLINE_PORTS[0]


def new_leader() -> int:
    """
    Elect new leader from available broker PORT's
    """
    global ONLINE_PORTS, LEADER_PORT

    ONLINE_PORTS.remove(LEADER_PORT)

    LEADER_PORT = ONLINE_PORTS[0]
    update_metadata(LEADER_PORT)

    return LEADER_PORT


def start_broker(port: int) -> None:
    """Start the broker server"""
    args: list = f"python yak/broker.py {port}".split(" ")

    subprocess.run(
        args,
        universal_newlines=True,
        shell=True,
        creationflags=subprocess.DETACHED_PROCESS,
    )


def check_status():
    """Periodically check the status of the leader"""
    global LEADER_PORT, ONLINE_PORTS
    try:
        _ = requests.get(f"http://localhost:{LEADER_PORT}/status")

    except ConnectionError:
        print(f"Leader is down - PORT: {LEADER_PORT}")

        # Elect new leader
        LEADER_PORT = new_leader()
        print(f"ONLINE PORTS - {ONLINE_PORTS}")

        # start broker on this newly elected leader
        start_broker(LEADER_PORT)


def init() -> None:
    """Start the leader"""
    args = f"python yak/broker.py {LEADER_PORT}".split(" ")
    subprocess.run(
        args,
        universal_newlines=True,
        shell=True,
        creationflags=subprocess.DETACHED_PROCESS,
    )


def main() -> None:

    print("Zookeeper has started")

    init()
    check_status()


if __name__ == "__main__":

    main()
