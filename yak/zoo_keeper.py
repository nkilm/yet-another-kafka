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
from constants import get_brokers, update_brokers, get_leader

ONLINE_PORTS: list = get_brokers()
LEADER_PORT: int = get_leader()


def new_leader() -> int:
    """
    Elect new leader from available broker PORT's
    """
    global ONLINE_PORTS, LEADER_PORT

    ONLINE_PORTS.remove(LEADER_PORT)
    update_brokers(ONLINE_PORTS)

    LEADER_PORT = get_leader()
    return LEADER_PORT


def start_broker(port: int) -> None:
    """Start the broker server"""
    args: list = f"python yak/broker.py {port}".split(" ")

    subprocess.run(
        args,
        universal_newlines=True,
        shell=False,
        creationflags=subprocess.CREATE_NEW_CONSOLE,
    )


def check_status():
    """Periodically check the status of the leader"""
    global LEADER_PORT, ONLINE_PORTS
    try:
        _ = requests.get(f"http://localhost:{LEADER_PORT}")

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
        shell=False,
        creationflags=subprocess.CREATE_NEW_CONSOLE,
    )


def main() -> None:

    print("Zookeeper has started")

    init()
    check_status()


if __name__ == "__main__":

    main()
