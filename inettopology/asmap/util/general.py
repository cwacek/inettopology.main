import itertools

def pairwise(iterable):
  "s -> (s0,s1), (s1,s2), (s2, s3), ..."
  a, b = itertools.tee(iterable)
  next(b, None)
  return itertools.izip(a, b)


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
