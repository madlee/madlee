from uuid import getnode

def local_mac_address():
    result = hex(getnode()).upper()
    return result[2:]

