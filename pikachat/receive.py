import pika, json

EXCHANGE_NAME = 'chat'

if __name__ == '__main__':
  connection = pika.BlockingConnection(
    pika.ConnectionParameters('172.17.42.1')
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