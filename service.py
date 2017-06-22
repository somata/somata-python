import time
import json
import threading
import requests
import signal
import sys
import os
import zmq
from .helpers import random_string
from .client import Client
context = zmq.Context()

# Constants
# ------------------------------------------------------------------------------

VERBOSE = os.environ.get('SOMATA_VERBOSE')

# Helpers
# ------------------------------------------------------------------------------

def log_msg(client_id, msg):
    if VERBOSE:
        print("[%s] <%s> ==> %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), client_id, msg))

# Remove empty values from dictionary
def prune_dict(d):
    return {k: v for k, v in d.items() if v}

# Service class
# ------------------------------------------------------------------------------

class Service:

    def __init__(self, name, methods, options):
        self.name = name
        self.id = self.name + '~' + random_string(8)
        self.methods = methods
        self.options = options
        self.subscriptions = {}

        # Create the binding socket
        self.socket = context.socket(zmq.ROUTER)
        self.socket.bind('tcp://0.0.0.0:%s' % options['bind_port'])

        # Deregister when killed
        signal.signal(signal.SIGINT, lambda signal, frame: self.deregister())

        self.poll = zmq.Poller()
        self.poll.register(self.socket, zmq.POLLIN)

        # Start the socket receive thread
        self.running = True
        self.recv_loop_thread = threading.Thread(target=self.socket_recv_loop)
        self.recv_loop_thread.start()

        # Start a thread for check passing
        # threading.Thread(target=self.pass_checks_loop).start()

        # Create registry client
        self.registry_client = Client({'connect_port': 8420}, 'registry')
        self.register()

        # Wait
        while self.running:
            self.recv_loop_thread.join(1)
            self.registry_client.recv_loop_thread.join(1)

    # Socket receive loop
    # --------------------------------------------------------------------------
    # Each message is a JSON object that should have a 'kind' attribute. If
    # there's a handler function for a given message kind, call it.

    def socket_recv_loop(self):
        while self.running:
            socks = dict(self.poll.poll(50))
            if self.socket in socks and socks[self.socket] == zmq.POLLIN:
                # Get client ID and message
                client_id = self.socket.recv()
                message = self.socket.recv_json()
                log_msg(client_id, message)

                if 'kind' not in message:
                    print("Invalid message: %s" % message)

                self.handle_message(client_id, message)

    def handle_message(self, client_id, message):
        handler_name = 'handle_' + message['kind']
        if hasattr(self, handler_name):
            handler = getattr(self, handler_name)
            handler(client_id, message)
        else:
            print("No handler for: %s" % message['kind'])

    # Handlers
    # --------------------------------------------------------------------------

    def handle_method(self, client_id, message):
        method = message['method']
        args = message['args']
        if method in self.methods:
            method_fn = self.methods[method]
            def respond(error, response=None):
                if error is not None:
                    print('send an error')
                    self.socket.send(client_id, zmq.SNDMORE)
                    self.socket.send_string(json.dumps({
                        "id": message['id'],
                        "kind": "error",
                        "error": error
                    }))
                else:
                    self.socket.send(client_id, zmq.SNDMORE)
                    self.socket.send_string(json.dumps({
                        "id": message['id'],
                        "kind": "response",
                        "response": response
                    }))
            args.append(respond)
            method_fn(*args)

    def handle_subscribe(self, client_id, message):
        event_type = message['type']
        new_subscription = {
            'id': message['id'],
            'client_id': client_id
        }
        if event_type in self.subscriptions:
            self.subscriptions[event_type].append(new_subscription)
        else:
            self.subscriptions[event_type] = [new_subscription]

    def handle_unsubscribe(self, client_id, message):
        event_type = message['type']
        without_client = lambda subs: [s for s in subs if s['client_id'] != client_id]
        self.subscriptions[event_type] = without_client(self.subscriptions[event_type])
        self.subscriptions = prune_dict(self.subscriptions)

    def handle_ping(self, client_id, message):
        if message['ping'] == 'hello':
            response = 'welcome'
        elif message['ping'] == 'ping':
            response = 'pong'
        else:
            print("Error: Unrecognized ping message of kind '%s'" % (message['kind']))
        self.socket.send(client_id, zmq.SNDMORE)
        self.socket.send_string(json.dumps({
            'id': message['id'],
            'kind': "pong",
            'pong': response
        }))

    # Event emitting
    # --------------------------------------------------------------------------

    def emit(self, event, data):
        print("===> %s: %s" % (event, data))
        if event in self.subscriptions:
            for subscription in self.subscriptions[event]:
                self.socket.send(subscription['client_id'], zmq.SNDMORE)
                self.socket.send_string(json.dumps({
                    "id": subscription['id'],
                    "kind": "event",
                    "event": data
                }))

    # Service registration and health checking
    # --------------------------------------------------------------------------

    def register(self):
        heartbeat = 0 if 'heartbeat' not in self.options else self.options['heartbeat']
        instance = {'id': self.id, 'name': self.name, 'port': self.options['bind_port'], 'heartbeat': heartbeat}
        def register_cb(register_response):
            print("Registered", register_response)
        self.registry_client.send_method('registerService', [instance], register_cb)

    def deregister(self):
        def deregister_cb(deregister_response):
            print("Deregistered", deregister_response)
            self.running = False
            self.registry_client.running = False
            sys.exit()
        self.registry_client.send_method('deregisterService', [self.id, self.name], deregister_cb)

