# Echo server program
import datetime
import socket
import selectors
import json
import pickle
import random
import xml.etree.ElementTree as ET
from topic import Topic
import time
import argparse


HOST = ''                 # Symbolic name meaning all available interfaces
# PORT = 8000               # Arbitrary non-privileged port
sel = selectors.DefaultSelector()
MU = 0

accepted_protocols = ["JSON", "XML", "PICKLE"]
protocols = {}
consumers = {}
topics = Topic("")
brokers = []


def accept(sock, mask):
    conn, addr = sock.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)  # regista nova socket


def send(conn, msg):
    _send(conn, pack_msg(protocols[conn], msg))


def _send(conn, msg):
    header = len(msg).to_bytes(16, byteorder="big")
    time.sleep(random.gauss(MU, MU*0.1))
    conn.sendall(header + msg.encode('utf-8'))


def pack_msg(protocol, raw_msg):
    if protocol == "JSON":
        msg = json.dumps(raw_msg)
    elif protocol == "XML":
        if 'value' in raw_msg.keys():
            msg = (
                '<?xml version="1.0"?><data op= "%(op)s" value= "%(value)s" topic= "%(topic)s"></data>' % raw_msg)
        elif 'type' in raw_msg.keys():
            msg = (
                '<?xml version="1.0"?><data op= "%(op)s" topic= "%(topic)s" type= "%(type)s"></data>' % raw_msg)
        elif 'list' in raw_msg.keys():
            msg = (
                '<?xml version="1.0"?><data op= "%(op)s" list= "%(list)s"></data>' % raw_msg)
    elif protocol == "PICKLE":
        msg = pickle.dumps(raw_msg)
    return msg


def read(conn, mask):
    header = conn.recv(16)
    msg_length = int.from_bytes(header, byteorder="big")
    raw_msg = conn.recv(msg_length).decode()
    if not raw_msg:
        return
    if raw_msg in accepted_protocols:
        register_protocol(conn, raw_msg)
    else:
        msg = unpack_msg(conn, raw_msg)
        if msg['op'] == "register":
            register_topic(conn, msg['topic'], msg['type'])
        elif msg['op'] == "publish":
            share(msg)
            publish(msg['topic'], msg["value"])
        elif msg['op'] == "unsubscribe":
            unsubscribe(conn)
        elif msg['op'] == "list_request":
            list_topics(conn)
        elif msg['op'] == "broker":
            add_broker(conn)


def add_broker(conn):
    brokers.append(conn)


def share(msg):
    if "from_broker" not in msg:
        for b in brokers:
            send(b, {
                 "op": "publish", "topic": msg["topic"], "value": msg["value"], "from_broker": True})


def unpack_msg(conn, raw_msg):
    if protocols[conn] == "JSON":
        msg = json.loads(raw_msg)
    elif protocols[conn] == "XML":
        t_msg = ET.fromstring(raw_msg)
        msg = t_msg.attrib
    elif protocols[conn] == "PICKLE":
        msg = pickle.loads(raw_msg)
    return msg


def register_protocol(conn, protocol):
    protocols[conn] = protocol


def register_topic(conn, topic, type):
    if type == "CONSUMER":
        if topic == "/":
            topic_path = ""
        else:
            topic_path = topic.split("/")

        topics.subscribe_topic(conn, topic_path)
        t = topics.get_topic_values(topic_path)
        for e in t:
            send(conn, {"op": "subscription", "topic": e[0], "value": e[1]})


def publish(topic, value):
    topics.set_topic_value(value, topic.split("/"))
    consumers_list = topics.find_consumers(topic.split("/"))
    for c in consumers_list:
        send(c, {"op": "subscription", "topic": topic, "value": value})


def list_topics(conn):
    t_list = topics.get_all_topic_values()
    send(conn, {"op": "list", "list": t_list})


def unsubscribe(conn):
    consumers.pop(conn)
    protocols.pop(conn)
    sel.unregister(conn)
    conn.close()


parser = argparse.ArgumentParser()
parser.add_argument(
    "--brokers", help="ports de brokers ativos ex: 8000 8001 8123", default=[], type=int, nargs='*')
parser.add_argument("--port", help="port ex '8001'", default="8000", type=int)
parser.add_argument("--mu", help="mu", default="0.1", type=float)
args = parser.parse_args()
MU = args.mu

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:

    for b in args.brokers:
        bconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        bconn.connect((HOST, b))
        protocols[bconn] = "JSON"
        _send(bconn, "JSON")  # registar protocolo entre brokers
        send(bconn, {"op": "broker"})

    s.bind((HOST, args.port))
    s.listen(100)
    s.setblocking(False)
    sel.register(s, selectors.EVENT_READ, accept)

    while True:
        events = sel.select()
        for key, mask in events:
            callback = key.data
            callback(key.fileobj, mask)
