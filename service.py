import time
import json
import threading
import requests
import signal
import sys
import zmq
context = zmq.Context()

# Helpers
# ------------------------------------------------------------------------------

def log_msg(client_id, msg):
    print("<< <%s>: %s" % (client_id, msg))

# Remove empty values from dictionary
def prune_dict(d):
    return {k: v for k, v in d.items() if v}

# Service class
# ------------------------------------------------------------------------------

class Service:

    def __init__(self, name, options):
        self.name = name
        self.options = options
        self.subscriptions = {}

        # Create the binding socket
        self.sock = context.socket(zmq.ROUTER)
        self.sock.bind('tcp://0.0.0.0:%s' % options['bind_port'])

        self.poll = zmq.Poller()
        self.poll.register(self.sock, zmq.POLLIN)

        self.register()

        # Start the socket receive thread
        threading.Thread(target=self.socket_recv_loop).start()

        # Start a thread for check passing
        threading.Thread(target=self.pass_checks_loop).start()

        # Deregister when killed
        signal.signal(signal.SIGINT, lambda signal, frame: self.deregister())

    # Socket receive loop
    # --------------------------------------------------------------------------
    # Each message is a JSON object that should have a 'kind' attribute. If
    # there's a handler function for a given message kind, call it.

    def socket_recv_loop(self):
        while True:
            socks = dict(self.poll.poll(1000))
            if self.sock in socks and socks[self.sock] == zmq.POLLIN:
                # Get client ID and message
                _id = self.sock.recv()
                msg = self.sock.recv_json()
                log_msg(_id, msg)

                if 'kind' not in msg:
                    print("Invalid message: %s" % msg)

                handler_name = 'handle_' + msg['kind']
                if hasattr(self, handler_name):
                    handler = getattr(self, handler_name)
                    handler(self.sock, _id, msg)
                else:
                    print("No handler for: %s" % msg['kind'])

    # Handlers
    # --------------------------------------------------------------------------

    def handle_subscribe(self, client_sock, client_id, msg):
        event_type = msg['type']
        new_subscription = {
            'id': msg['id'],
            'client_id': client_id
        }
        if event_type in self.subscriptions:
            self.subscriptions[event_type].append(new_subscription)
        else:
            self.subscriptions[event_type] = [new_subscription]

    def handle_unsubscribe(self, client_sock, client_id, msg):
        print("<< <%s>: %s" % (client_id, msg))
        event_type = msg['type']
        without_client = lambda subs: [s for s in subs if s['client_id'] != client_id]
        self.subscriptions = {
            event_type: without_client(subs)
            for event_type, subs in self.subscriptions.items()
        }
        self.subscriptions = prune_dict(self.subscriptions)

    # Event emitting
    # --------------------------------------------------------------------------

    def emit(self, event, data):
        print("%s: %s" % (event, data))
        if event in self.subscriptions:
            for subscription in self.subscriptions[event]:
                self.sock.send(subscription['client_id'], zmq.SNDMORE)
                self.sock.send_string(json.dumps({
                    "id": subscription['id'],
                    "kind": "event",
                    "event": data
                }))

    # Service registration and health checking
    # --------------------------------------------------------------------------

    def register(self):
        register_request = requests.put('http://localhost:8500/v1/agent/service/register', data=json.dumps({
            'Name': self.name,
            'Port': self.options['bind_port'],
            'Check': {
                'Interval': 60,
                'TTL': '10s'
            }
        }), headers={'content-type': 'application/json'})
        print("Registered", register_request)

    def pass_check(self):
        check_id = 'service:' + self.name
        check_request = requests.get('http://localhost:8500/v1/agent/check/pass/%s' % check_id)
        print("Checked", check_request)

    def pass_checks_loop(self):
        while True:
            time.sleep(5)
            self.pass_check()

    def deregister(self):
        deregister_request = requests.get('http://localhost:8500/v1/agent/service/deregister/%s' % self.name)
        print("Deregistered", deregister_request)
        sys.exit()

