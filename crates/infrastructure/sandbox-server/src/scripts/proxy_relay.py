"""Loopback proxy listener; transport is an attached Docker exec, never an NIC.
Frames: kind:u8, connection:u32be, length:u32be, payload. All queues are bounded.
"""
import os
import selectors
import socket
import struct
import sys

MAX_FRAME = 32768
MAX_CONNECTIONS = 16
selector = selectors.DefaultSelector()
listener = socket.socket()
listener.bind(('127.0.0.1', 18080))
listener.listen(MAX_CONNECTIONS)
listener.setblocking(False)
selector.register(listener, selectors.EVENT_READ)
selector.register(sys.stdin.buffer, selectors.EVENT_READ)
connections = {}
buffer = bytearray()
sequence = 0

def send(kind, key, payload=b''):
    sys.stdout.buffer.write(struct.pack('!BII', kind, key, len(payload)) + payload)
    sys.stdout.buffer.flush()

def close(key):
    item = connections.pop(key, None)
    if item:
        selector.unregister(item)
        item.close()

send(0, 0)
while True:
    for selected, _ in selector.select():
        if selected.fileobj is listener:
            conn, _ = listener.accept()
            if len(connections) >= MAX_CONNECTIONS:
                conn.close()
                continue
            sequence += 1
            conn.settimeout(5)
            connections[sequence] = conn
            selector.register(conn, selectors.EVENT_READ, sequence)
            send(1, sequence)
        elif selected.fileobj is sys.stdin.buffer:
            data = os.read(sys.stdin.fileno(), 65536)
            if not data:
                sys.exit(0)
            buffer.extend(data)
            while len(buffer) >= 9:
                kind, key, length = struct.unpack('!BII', buffer[:9])
                if length > MAX_FRAME:
                    sys.exit(2)
                if len(buffer) < 9 + length:
                    break
                payload = bytes(buffer[9:9+length])
                del buffer[:9+length]
                if kind == 3:
                    close(key)
                    send(3, key)
                elif kind == 2 and key in connections:
                    try:
                        connections[key].sendall(payload)
                    except OSError:
                        close(key)
                        send(3, key)
        else:
            key = selected.data
            try:
                data = selected.fileobj.recv(MAX_FRAME)
            except OSError:
                data = b''
            if data:
                send(2, key, data)
            else:
                close(key)
                send(3, key)
