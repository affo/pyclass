from distop.ops import DistOp
import logging, time, argparse

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

class PowOp(DistOp):
  OP_NAME = 'pow'

  def gather(self, partials):
    no_el = sum([len(p) for p in partials.values()])
    res = [None for _ in xrange(no_el)]

    for i, chunk in partials.iteritems():
      res[i:i+len(chunk)] = chunk

    return res

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--rabbit_host', type=str, help='Rabbit host IP',
                      default='localhost')
  parser.add_argument('--group_size', type=int, help='Number of servers',
                      default=1)

  args = parser.parse_args()

  op = PowOp(
    data=list(xrange(30)),
    group_size=args.group_size,
    rabbit_host=args.rabbit_host
  )

  op.call(esp=2)

  another_op = PowOp(
    data=list(xrange(20)),
    group_size=args.group_size,
    rabbit_host=args.rabbit_host
  )

  another_op.call(esp=3)

  for _ in xrange(4):
    LOG.info('I sleep because I can get the result when I want to.')
    time.sleep(1)

  LOG.info('Result: {}'.format(op.get_result()))
  LOG.info('Result: {}'.format(another_op.get_result()))