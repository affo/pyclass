import pika
from threading import Thread
import random, json
from pika import exceptions

EXCHANGE_NAME = 'chat'
with open('pikachat/pokenames.json') as file_data:
  USERNAMES = json.load(file_data).values()

username = random.choice(USERNAMES) + str(int(random.random() * 1000))

print 'Chat entered as ' + username

connection = pika.BlockingConnection(pika.ConnectionParameters('172.17.42.1'))
channel = connection.channel()

# Creating a queue using queue_declare is idempotent:
# we can run the command as many times as we like,
# and only one will be created.
queue = channel.queue_declare(exclusive=True) # once we disconnect the consumer the queue should be deleted
channel.exchange_declare(exchange=EXCHANGE_NAME, type='fanout')

def consume():
  queue = channel.queue_declare(exclusive=True) # new queue
  channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue.method.queue) # bind it to exchange

  def callback(ch, method, properties, body):
    print body

  channel.basic_consume(callback, queue=queue.method.queue, no_ack=True)
  try:
    channel.start_consuming()
  except exceptions.ConnectionClosed:
    # ok, connection is closed,
    # let's stop this thread
    pass
  return #stops the consumer thread

th = Thread(target=consume, args=[])
th.start()

msg = ''
while msg != '\quit':
  msg = raw_input()
  body = username + ':' + msg
  channel.basic_publish(
    exchange=EXCHANGE_NAME,
    routing_key='',
    body=body
  )

try:
  connection.close()
except RuntimeError:
  # https://github.com/pika/pika/pull/500
  # open issue
  pass

print "Chat exited"
