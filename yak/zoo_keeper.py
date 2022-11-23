#!/usr/bin/env python3

"""
Functions of zookeeper
- keep track of the leader - Make contacts regularly with the leader
- Elect leader from available brokers
- always running

"""
import subprocess
from random import choice

import requests
from requests.exceptions import ConnectionError

ONLINE_PORTS: list = [6060, 7070, 8080]
OFFLINE_PORTS: list = None

LEADER_PORT: int = choice(ONLINE_PORTS)


def new_leader(offline_ports: list, online_ports: list) -> int:
    """
    Elect new leader from available broker PORT's
    """
    offline_ports.append(LEADER_PORT)
    online_ports.remove(LEADER_PORT)

    LEADER_PORT = choice(online_ports)
    return LEADER_PORT


def start_broker(port: int) -> None:
    """Start the broker server"""
    args: list = f"python broker.py {port}".split(" ")

    subprocess.run(
        args,
        universal_newlines=True,
        shell=False,
    )


def check_status():
    """Periodically check the status of the leader"""
    try:
        _ = requests.get(f"http://localhost:{LEADER_PORT}")

    except ConnectionError:
        print(f"Leader is down - PORT: {LEADER_PORT}")

        # Elect new leader
        LEADER_PORT = new_leader(OFFLINE_PORTS, ONLINE_PORTS)

        # start broker on this newly elected leader
        start_broker(LEADER_PORT)


def init() -> None:
    """Start the leader"""
    args = f"python broker {LEADER_PORT}".split(" ")
    subprocess.run(
        args,
        universal_newlines=True,
        shell=False,
    )


def main() -> None:

    print("Zookeeper has started")

    init()
    check_status()


if __name__ == "__main__":
    main()
