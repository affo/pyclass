import pika, json, sys

EXCHANGE_NAME = 'chat'

if __name__ == '__main__':
  rabbit_host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'

  connection = pika.BlockingConnection(
    pika.ConnectionParameters(rabbit_host)
  )
  channel = connection.channel()

  queue = channel.queue_declare(exclusive=True)
  channel.exchange_declare(exchange=EXCHANGE_NAME, type='fanout')
  # binds the unnamed queue to the exchange
  channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue.method.queue)

  def on_message(ch, method, properties, body):
    print body
    ch.basic_ack(delivery_tag=method.delivery_tag)

  channel.basic_consume(on_message, queue=queue.method.queue)
  try:
    channel.start_consuming()
  except KeyboardInterrupt:
    channel.stop_consuming()
  finally:
    connection.close()