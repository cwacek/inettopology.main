import functools
import collections


class singleton(object):
  def __init__(self, decorated):
    self._decorated = decorated

  def Instance(self):
    try:
      return self._instance
    except AttributeError:
      self._instance = self._decorated()
      return self._instance

  def __call__(self):
    return self.Instance()()
    #raise TypeError("Singletons must be accessed through `Instance()`.")

  def __instancecheck__(self, inst):
    return isinstance(inst, self._decorated)


class factory(object):
  """ Decorate a function so that it becomes a cached factory."""

  def __init__(self, func):
    self.func = func
    self.cache = {}

  def __call__(self, *args):
    if not isinstance(args, collections.Hashable):
      return self.func(args)
    if args in self.cache:
      return self.cache[args]
    else:
      value = self.func(*args)
      self.cache[args] = value
      return value

  def __doc__(self):
    return self.func.__doc__

  def __get__(self, obj, objtype):
    '''Support instance methods'''
    return functools.partial(self.__call__, obj)
