import itertools
import time


def pairwise(iterable):
  "s -> (s0,s1), (s1,s2), (s2, s3), ..."
  a, b = itertools.tee(iterable)
  next(b, None)
  return itertools.izip(a, b)


def triwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b, c = itertools.tee(iterable, 3)
    next(b, None)
    next(c, None)
    next(c, None)
    return itertools.izip(a, b, c)


def uniqify(seq, key=None, stopat=lambda x: False):
  """ Return an order-preserving sequence containing
  only unique values from :seq:.

  :key: can be a function which returns the value
  to compare from within the element.

  If :stopat: is supplied, it will be called on every
  element in the sequence. If it returns a truth
  value, iteration will stop at that point.
  """
  # order preserving
  if key is None:
    def key(x):
      return x
  seen = {}
  result = []
  for item in seq:
    if stopat(item):
      break
    marker = key(item)
    # in old Python versions:
    # if seen.has_key(marker)
    # but in new ones:
    if marker in seen:
      continue
    seen[marker] = 1
    result.append(marker)
  return result


def confirm(prompt=None):
  """ prompts for yes or no response from the user.
  Returns True for yes and False for no.
  """
  if prompt is None:
    prompt = 'Confirm'

  prompt = '%s %s|%s|%s: ' % (prompt, 'y', 'n', 'a')

  while True:
    ans = raw_input(prompt)
    if ans not in ['y', 'Y', 'n', 'N', 'A', 'a']:
      print 'please enter y or n or a (to ignore all further prompts)'
      continue
    if ans == 'y' or ans == 'Y':
      return (True, False)
    if ans == 'n' or ans == 'N':
      return (False, False)
    if ans == 'a' or ans == 'A':
      return (True, True)


class ProgressTimer:
    def __init__(self, total):
        self.init_time = time.time()
        self.total = total
        self.total_done = 0
        self._last_tick = 0

    def tick(self, count):
        """
        Tick the timer, assuming that <b>count</b> 'things'
        have been done.
        """
        self.total_done += count
        self._last_tick = time.time()

    def eta(self):
        """
        Return the expected finish time in seconds.
        """
        try:
            avg = self.total_done / (self._last_tick - self.init_time)
            eta = int((self.total - self.total_done) / avg)
        except ZeroDivisionError:
            eta = -1
        return eta

    def elapsed(self):
        return time.time() - self.init_time


class Color:
    HEADER = '\033[95m'
    OKBLUE = '\033[34m'
    OKGREEN = '\033[32m'
    FAIL = '\033[91m'
    NEWL = "\r\x1b[K"
    ENDC = '\033[0m'

    @classmethod
    def fail(self, text):
        return Color.FAIL + text + Color.ENDC

    @classmethod
    def wrap(self, text, color):
      return color + str(text) + Color.ENDC

    @classmethod
    def wrapformat(self, fmt, color, *args, **kwargs):
      return color + fmt.format(*args, **kwargs) + Color.ENDC
