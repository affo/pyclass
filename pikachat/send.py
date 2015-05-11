import pika, random, json, logging, sys
from pikachat.receive import EXCHANGE_NAME

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

if __name__ == '__main__':
  with open('pikachat/pokenames.json') as file_data:
    USERNAMES = json.load(file_data).values()
  username = random.choice(USERNAMES) + str(int(random.random() * 1000))

  LOG.info('Chat entered as ' + username)
  LOG.info('Please execute `python receive.py` to receive messages...')

  rabbit_host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'

  connection = pika.BlockingConnection(
    pika.ConnectionParameters(rabbit_host)
  )
  channel = connection.channel()

  queue = channel.queue_declare(exclusive=True)
  # exclusive = True -> once we disconnect the consumer,
  # the queue should be deleted.
  # No need to give it a name. We are only interested
  # in the exchange name! The consumer will be bound
  # to the exchange
  channel.exchange_declare(exchange=EXCHANGE_NAME, type='fanout')

  try:
    while True:
      msg = raw_input()
      body = username + ':' + msg
      channel.basic_publish(
        exchange=EXCHANGE_NAME, # publish to the exchange
        routing_key='', # no named queue!
        body=body
      )
  except KeyboardInterrupt:
    pass

  connection.close()

  print 'Chat exited'