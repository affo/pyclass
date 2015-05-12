from distop.ops import RPC_QUEUE_NAME
import pika, logging, json, socket

_HOSTNAME = socket.gethostname()

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
    q = self._channel.queue_declare(RPC_QUEUE_NAME)
    self._q_name = q.method.queue

    self._channel.basic_consume(
      self.on_op,
      queue=self._q_name
    )

  def on_op(self, ch, method, props, body):
    body = json.loads(body)

    LOG.info(str(body))

    chunk = body['chunk']
    i = body['start_index']
    op_name = body['op_name']
    params = body['params']
    corr_id = props.correlation_id

    res = self.execute(op_name, chunk, params)

    self._channel.basic_ack(delivery_tag=method.delivery_tag)
    response = {
      'result': res,
      'start_index': i,
      'hostname': _HOSTNAME,
    }
    self._channel.basic_publish(
      exchange='',
      routing_key=props.reply_to,
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