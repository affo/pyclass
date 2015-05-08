from distop import ops
import logging, time, argparse
logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--rabbit_host', type=str, help='Rabbit host IP',
                      default='localhost')
  parser.add_argument('--group_size', type=int, help='Number of servers',
                      default=1)

  args = parser.parse_args()

  op = ops.PowOp(
    data=[1, 2, 3, 4, 5, 6, 7],
    group_size=args.group_size,
    rabbit_host=args.rabbit_host
  )

  op.call(esp=2)

  for _ in xrange(4):
    LOG.info('I sleep because I can get the result when I want to.')
    time.sleep(1)

  LOG.info('Result: {}'.format(op.get_result()))