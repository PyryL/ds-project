import socket
import argparse

def read_value(key: int, ip_addr: str) -> bytes:
    """
    Read a value of the given key from the datastore.

    :param key: The key whose value to read.
    :param ip_addr: The IP address of any node in the datastore system.
    """

    # send request
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip_addr, 52525))
    s.sendall(bytes([200]) + int.to_bytes(13, 4) + int.to_bytes(key, 8))

    # receive the header of the message and get the message length
    header = s.recv(5)
    if header[0] != 0:
        raise ValueError("unexpected response message type")
    msg_length = int.from_bytes(header[1:5])

    # read the responded value
    value = s.recv(msg_length - 5)
    s.close()

    return value

def write_value(key: int, new_value: bytes | None, ip_addr: str) -> None:
    """
    Writes a new value for the given key.

    :param key: The key that identifies the value.
    :param new_value: The new value bytes. If this is `None`, then new value is
        read from the stdin after receiving the old value.
    :param ip_addr: The IP address of any node in the datastore system.
    """

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip_addr, 52525))

    # send request
    s.sendall(bytes([202]) + int.to_bytes(13, 4) + int.to_bytes(key, 8))

    # wait for the permission
    permission_msg_header = s.recv(5)
    if permission_msg_header[0] != 0:
        raise ValueError("malformed permission")
    old_value_length = int.from_bytes(permission_msg_header[1:5]) - 5
    old_value = s.recv(old_value_length)

    print('old value was', old_value)

    if new_value is None:
        new_value = input('new value: ').encode()

    # send the new value
    new_request_length = len(new_value) + 5
    s.sendall(bytes([0]) + int.to_bytes(new_request_length, 4) + new_value)

    # check the acknowledgement
    ack_message = s.recv(7)
    if ack_message != bytes([0]) + int.to_bytes(7, 4) + bytes([111, 107]):
        raise ValueError("malformed ack response")

def parse_args():
    parser = argparse.ArgumentParser(description='A sample client for accessing the key-value store')

    parser.add_argument('nodeip', help='IP address of any node in the datastore')

    subparsers = parser.add_subparsers(dest='action', required=True, help='action to perform')

    parser_r = subparsers.add_parser('r', help='read the value of a given key')
    parser_r.add_argument('key', type=int, help='the key whose value to read')

    parser_w = subparsers.add_parser('w', help='write the value for a given key')
    parser_w.add_argument('key', type=int, help='the key whose value to write')
    parser_w.add_argument('value', nargs='?', help='the value to write (default: stdin)')

    return parser.parse_args()

def main():
    args = parse_args()

    if args.action == 'r':
        value = read_value(args.key, args.nodeip)
        print(value)

    elif args.action == 'w':
        value = None if args.value is None else args.value.encode()
        write_value(args.key, value, args.nodeip)

if __name__ == '__main__':
    main()
