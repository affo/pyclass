from oslo import messaging
from oslo_config import cfg

TRANSPORT = None

def init():
  global TRANSPORT
  if TRANSPORT is not None: return
  TRANSPORT = messaging.get_transport(cfg.CONF)

def get_client(topic):
  assert TRANSPORT is not None
  target = messaging.Target(topic=topic)
  return messaging.RPCClient(TRANSPORT, target)

import socket
def get_server(topic, endpoints):
  assert TRANSPORT is not None
  assert type(endpoints) is list
  target = messaging.Target(topic=topic, server=socket.gethostname())
  return messaging.get_rpc_server(TRANSPORT, target, endpoints)

def get_notifier(publisher_id, topic):
  assert TRANSPORT is not None
  return messaging.Notifier(
    TRANSPORT,
    publisher_id=publisher_id,
    driver='messaging',
    topic=topic
  )

def get_notification_listener(topic, endpoints):
  assert TRANSPORT is not None
  targets = [messaging.Target(topic=topic)]
  return messaging.get_notification_listener(TRANSPORT, targets, endpoints)
