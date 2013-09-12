import functools 
import collections


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
