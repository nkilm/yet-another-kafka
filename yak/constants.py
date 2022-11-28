ONLINE_PORTS: list = sorted([6060, 7070, 8080])


def get_brokers():
    return ONLINE_PORTS


def get_leader():
    return ONLINE_PORTS[0]


def update_brokers(new_ports):
    global ONLINE_PORTS
    ONLINE_PORTS = new_ports
