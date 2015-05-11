import pika
import logging
logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

def on_message(channel, method_frame, header_frame, body):
  LOG.info('Received message #{}: {}'.format(
      method_frame.delivery_tag,
      body
    )
  )
  channel.basic_ack(delivery_tag=method_frame.delivery_tag)

connection = pika.BlockingConnection(
  pika.ConnectionParameters()
)
channel = connection.channel()
q = channel.queue_declare('pc')
q_name = q.method.queue
channel.basic_consume(on_message, q_name)

try:
  channel.start_consuming()
except KeyboardInterrupt:
  channel.stop_consuming()
finally:
  connection.close()