from distop.ops import EXCHANGE_NAME, OP_TOPIC, RES_TOPIC
import pika, logging, json

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

class OpExecutor(object):

  def __init__(self, rabbit_host='localhost'):
    super(OpExecutor, self).__init__()

    # pika stuff
    self._connection = pika.BlockingConnection(
      pika.ConnectionParameters(rabbit_host)
    )
    self._channel = self._connection.channel()
    q = self._channel.queue_declare(exclusive=True)
    self._q_name = q.method.queue
    self._channel.exchange_declare(EXCHANGE_NAME, type='topic')
    self._channel.queue_bind(
      exchange=EXCHANGE_NAME,
      queue=self._q_name,
      routing_key=OP_TOPIC + '.*'
    )

    self._channel.basic_consume(
      self.on_op,
      queue=self._q_name
    )

  def on_op(self, ch, method, props, body):
    body = json.loads(body)

    LOG.info(str(body))

    chunk = body['chunk']
    i = body['start_index']
    params = body['params']
    op_name = method.routing_key.split('.')[1]
    corr_id = props.correlation_id

    res = self.execute(op_name, chunk, params)

    self._channel.basic_ack(delivery_tag=method.delivery_tag)
    response = {
      'result': res,
      'start_index': i
    }
    self._channel.basic_publish(
      exchange=EXCHANGE_NAME,
      routing_key=RES_TOPIC,
      properties=pika.BasicProperties(
        correlation_id=corr_id
      ),
      body=json.dumps(response)
    )

  def execute(self, op_name, chunk, param_dict):
    try:
      op = getattr(self, op_name)
      res = op(chunk, **param_dict)
      return res
    except AttributeError:
      LOG.error('No implementation for op {}'.format(op_name))
      return None # should be handled better...

  def run(self):
    try:
      self._channel.start_consuming()
    except KeyboardInterrupt:
      self._channel.stop_consuming()
    finally:
      self._connection.close()


class MathOpExecutor(OpExecutor):

  def nop(self, chunk):
    return chunk

  def pow(self, chunk, esp=1):
    return [el ** esp for el in chunk]

  def plusone(self, chunk):
    return [el + 1 for el in chunk]


if __name__ == '__main__':
  import sys
  rabbit_host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
  ex = MathOpExecutor(rabbit_host)
  ex.run()