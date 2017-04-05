import time
import json
import threading
import requests
import signal
import sys
import zmq
from .helpers import random_string
context = zmq.Context()

# Helpers
# ------------------------------------------------------------------------------

def log_msg(client_id, msg):
    print("[%s] <%s> <== %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), client_id, msg))

# Service class
# ------------------------------------------------------------------------------

class Client:

    def __init__(self, options, service):
        self.id = random_string(8)
        self.options = options
        self.connections = {}
        self.pending = {}
        self.service = service

        # Create the binding socket
        self.socket = context.socket(zmq.DEALER)
        self.socket.setsockopt_string(zmq.IDENTITY, unicode(self.id))
        self.socket.connect('tcp://127.0.0.1:%s' % options['connect_port'])

        self.poll = zmq.Poller()
        self.poll.register(self.socket, zmq.POLLIN)

        # Start the socket receive thread
        self.running = True
        self.recv_loop_thread = threading.Thread(target=self.socket_recv_loop)
        self.recv_loop_thread.start()

    # Socket receive loop
    # --------------------------------------------------------------------------
    # Each message is a JSON object that should have a 'kind' attribute. If
    # there's a handler function for a given message kind, call it.

    def socket_recv_loop(self):
        while self.running:
            socks = dict(self.poll.poll(1000))
            if self.socket in socks and socks[self.socket] == zmq.POLLIN:
                # Get client ID and message
                message = self.socket.recv_json()
                log_msg(self.id, message)

                if 'kind' not in message:
                    print("Invalid message: %s" % message)

                self.handle_message(message)

    def handle_message(self, message):
        if message['kind'] == 'response':
            cb = self.pending[message['id']]
            cb(message['response'])

    def send(self, message, cb):
        message['id'] = random_string(16)
        self.pending[message['id']] = cb
        self.socket.send_json(message)

    def send_method(self, method, args, cb):
        self.send({'kind': 'method', 'method': method, 'args': args, 'service': self.service}, cb)

