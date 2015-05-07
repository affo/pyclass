import pika, logging, sys

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

connection = pika.BlockingConnection(
	pika.ConnectionParameters('172.17.42.1')
)
channel = connection.channel()
q = channel.queue_declare('pc')
q_name = q.method.queue

# Turn on delivery confirmations
channel.confirm_delivery()

message = ' '.join(sys.argv[1:]) or "Hello World!"

if channel.basic_publish('', q_name, message):
	LOG.info('Message has been delivered')
else:
	LOG.warning('Message NOT delivered')

connection.close()