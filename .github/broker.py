# Echo server program
import datetime
import socket
import selectors
import json
import pickle
import xml.etree.ElementTree as ET

HOST = ''                 # Symbolic name meaning all available interfaces
PORT = 8000               # Arbitrary non-privileged port
sel = selectors.DefaultSelector()

accepted_protocols = ["JSON", "XML", "PICKLE"]
protocols = {}
consumers = {}
last_value = {}


def accept(sock, mask):
    conn, addr = sock.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)  # regista nova socket


def send(conn, msg):
    m = pack_msg(protocols[conn],
                 msg).replace("\t", "    ") + "\t"
    conn.sendall(m.encode('utf-8'))


def pack_msg(protocol, raw_msg):
    if protocol == "JSON":
        msg = json.dumps(raw_msg)
    elif protocol == "XML":
        parser = ET.XMLPullParser(['start', 'end'])
        parser.feed(raw_msg)
    elif protocol == "PICKLE":
        msg = pickle.dumps(raw_msg)
    return msg


def read(conn, mask):
    raw_msg = ""
    while True:
        data = conn.recv(1).decode()
        if not data:
            return
        if data == "\t":
            break
        else:
            raw_msg = raw_msg + data
    print(f"RECEIVED: {raw_msg}")

    if raw_msg in accepted_protocols:
        register_protocol(conn, raw_msg)
    else:
        msg = unpack_msg(conn, raw_msg)
        if msg['op'] == "register":
            register_topic(conn, msg['topic'], msg['type'])
        elif msg['op'] == "publish":
            publish(msg['topic'], msg["value"])


def unpack_msg(conn, raw_msg):
    if protocols[conn] == "JSON":
        msg = json.loads(raw_msg)
    elif protocols[conn] == "XML":
        parser = ET.XMLPullParser(['start', 'end'])
        parser.read_events(raw_msg)
    elif protocols[conn] == "PICKLE":
        msg = pickle.loads(raw_msg)
    return msg


def register_protocol(conn, protocol):
    protocols[conn] = protocol


def register_topic(conn, topic, type):
    if type == "CONSUMER":
        consumers[conn] = topic
        if topic not in last_value:
            t = None
        else:
            t = last_value[topic]
        send(conn, {"op": "subscription", "topic": topic, "value": t})


def publish(topic, value):
    last_value[topic] = value
    for c in consumers:
        if consumers[c] in topic:
            send(c, {"op": "subscription", "topic": topic, "value": value})


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen(100)
    s.setblocking(False)
    sel.register(s, selectors.EVENT_READ, accept)

    while True:
        events = sel.select()
        for key, mask in events:
            callback = key.data
            callback(key.fileobj, mask)
