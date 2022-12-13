from uuid import getnode

def local_mac_address():
    result = hex(getnode()).upper()
    return result[2:]

def hostname():
    import os
    hostname = os.environ.get('HOSTNAME', None)
    if hostname == None:
        import socket
        hostname = socket.gethostname()
    return hostname

    