import multiprocessing
import socket
import time
import redis
import redis.connection
import argparse
import logging
log = logging.getLogger(__name__)

__all__ = ["Collection", "ProcessingQueue", "KeyedCollection", "Logger"]


class RedisArgAction(argparse.Action):
  def __call__(self, parser, namespace, values, option_string=None):
    args = [conv(arg) for conv, arg in zip((str, int, int), values.split(":"))]
    setattr(namespace, self.dest, dict(zip(('host', 'port', 'db'), args)))


class Collection(object):

  add_lua = """
  local sadd_result
  sadd_result = redis.call("SADD", KEYS[1], ARGV[1])
  if sadd_result == 1 then
    redis.call("LPUSH", KEYS[2], ARGV[1])
  end
  return sadd_result
  """

  def __init__(self, redis, prefix):
    self._prefix = prefix
    self._r = redis
    self._add_script = redis.register_script(Collection.add_lua)

  def __len__(self):
    return self._r.scard(self._set())

  def _attrs(self, element):
    return "collection:{0}:attr:{1}".format(self._prefix, element)

  def _set(self):
    return "collection:{0}:set".format(self._prefix)

  def _list(self):
    return "collection:{0}:list" .format(self._prefix)

  def __contains__(self, key):
    return True if self._r.sismember(self._set(), key) else False

  def delete(self, pipe=None):
    r = pipe if pipe else self._r
    for element in self:
      r.delete(self._attrs(element))
    r.delete(self._set())
    r.delete(self._list())

  def exists(self):
    return self._r.exists(self._set()) or self._r.exists(self._list())

  def add(self, elements, attrs=None, pipe=None):
    rconn = pipe if pipe else self._r.pipeline()
    for i, element in enumerate(elements):
      self._add_script(keys=[self._set(), self._list()],
                       args=[element],
                       client=rconn)
      if attrs:
        rconn.hmset(self._attrs(element), attrs[i])
    if rconn == pipe:
      return "pipelined"  # will be executed elsewhere

    result = rconn.execute()
    return sum(result)

  def add_attrs(self, element, attrdict, pipe=None):
    """
    Append to the attributes for a given element.
    """
    r = pipe if pipe else self._r
    r.hmset(self._attrs(element), attrdict)
    #elem, old_attrs = self.get(element, r if pipe else None)

    #old_attrs.update(attrdict)
    #r.hmset(self._attrs(element), old_attrs)

  def get(self, element, pipe=None):
    """
    Return a tuple containing a specific element and its
    attributes.
    """
    r = pipe if pipe else self._r
    if not r.sismember(self._set(), element):
      raise KeyError("{0} does not exist in collection".format(element))

    if not self._attrs:
      return (element, None)
    else:
      return (element, r.hgetall(self._attrs(element)))

  def remove(self, element):
    """
    Remove a specific element
    """

    if not self._r.sismember(self._set(), element):
      return

    self._r.lrem(self._list(), 0, element)
    self._r.srem(self._set(), element)

  def get_attr(self, element, attr_key, pipe=None):
    """
    Return a specific attribute belonging to an element, or
    None if it does not exist.
    """
    if pipe:
      pipe.hget(self._attrs(element), attr_key)
      return pipe
    else:
      return self._r.hget(self._attrs(element), attr_key)

  def members(self):
    """
    Return the set of members of this collection.
    If there are none, return an empty list
    """
    v = self._r.smembers(self._set())
    if not v:
      return set()
    return v

  def __iter__(self):
    return CollectionIterator(self._r, self._list())


class KeyedCollection(Collection):

  def __init__(self, redis, prefix):
    self._base_prefix = prefix
    Collection.__init__(self, redis, prefix)

  def members(self, key):
    self._prefix = self._base_prefix + ":" + key
    return Collection.members(self)

  def add(self, key, elements, attrs=None, pipe=None):
    self._prefix = self._base_prefix + ":" + key
    return Collection.add(self, elements, attrs, pipe)

  def delete(self, key):
    self._prefix = self._base_prefix + ":" + key
    for element in self.foreach(key):
      self._r.delete(self._attrs(element))
    self._r.delete(self._set())
    self._r.delete(self._list())

  def get(self, key, element):
    self._prefix = self._base_prefix + ":" + key
    return Collection.get(self, element)

  def foreach(self, key):
    self._prefix = self._base_prefix + ":" + key
    it = CollectionIterator(self._r, self._list())
    while True:
      yield it.next()

  def __iter__(self):
    raise Exception("Use .foreach")


class CollectionIterator(object):
  def __init__(self, redis, list_key):
    self.idx = redis.llen(list_key)
    self._r = redis
    self._l = list_key

  def next(self):
    if self.idx < 1:
      raise StopIteration
    else:
      self.idx -= 1
      return self._r.rpoplpush(self._l, self._l)


class PriorityQueue(object):

  def __init__(self, rinfo, prefix):
    self._set = "prioqueue:{0}:set".format(prefix)
    self.rinfo = rinfo
    self._r = rinfo.instantiate()

  def add(self, element, priority):
    self._r.zadd(self._set, priority, element)

  def pop(self):
    """
    Pop and return the smallest element
    """
    elem = self._r.zrange(self._set, 0, 0)
    self._r.zrem(self._set, elem)
    return elem

  def peek(self):
    """
    Return the smallest element but leave it in
    """
    return self._r.zrange(self._set, 0, 0)

  def __len__(self):
    return self._r.zcard(self._set)


class ProcessingQueue(object):

  def __init__(self, r, prefix, track_seen=True, is_listener=False):
    self._do_list = "procqueue:{0}:list".format(prefix)
    self._done_list = "procqueue:{0}:done".format(prefix)
    self._set = "procqueue:{0}:set".format(prefix)
    self._unique_entry_set = "procqueue:{0}:infilter".format(prefix)
    self.listener_key = "procqueue:{0}:meta:have_listener".format(prefix)
    self.track_seen = track_seen

    if isinstance(r, redis.Redis):
      self._redis = r
    elif isinstance(r, ConnectionInfo):
      self._redis = r.instantiate()
    else:
      raise TypeError("Expected Redis Connection or ConnectionInfo")

    self._add_script = self._redis.register_script(Collection.add_lua)

  def was_processed(self, element):
    return True if self._redis.sismember(self._set, element) == 1 else False

  def has_listeners(self):
    try:
      result = int(self._redis.get(self.listener_key))
      return True if result > 0 else False
    except TypeError:
      return False

  def reset(self):
    self._redis.delete(self._set)
    self._redis.delete(self._unique_entry_set)
    self._redis.delete(self._done_list)
    self._redis.delete(self._do_list)

  def get_next(self):
    """
    Retrieves the next element from the processing list
    that has not already been seen. Adds that element
    to the processed list.

    If the list is empty, returns None
    """
    element = self._redis.rpop(self._do_list)
    if element is None:
      return None
    if self.track_seen:
      while self._redis.sadd(self._set, element) == 0:
        element = self._redis.rpop(self._do_list)
        if element:
          self._redis.srem(self._unique_entry_set(element))
      self._redis.lpush(self._done_list, element)

    self._redis.srem(self._unique_entry_set(element))
    return element

  def add(self, element, pipe=None):
    return self._add_script(keys=[self._unique_entry_set, self._do_list],
                            args=[element],
                            client=pipe)

  def add_from(self, elements):
    pipe = self._redis.pipeline()
    for elem in elements:
      self.add(elem, pipe)
    return pipe.execute()

  def __len__(self):
    return self._redis.llen(self._do_list)

  def num_processed(self):
    return self._redis.scard(self._set)


class ConnectionInfo(object):

  def __init__(self, **kwargs):
    self.port = kwargs['port'] if 'port' in kwargs else '6379'
    self.host = kwargs['host'] if 'host' in kwargs else 'localhost'
    self.db = kwargs['db'] if 'db' in kwargs else 'db'
    if 'socket' in kwargs:
      redis.connection.socket = kwargs['socket']
      self.socket_type = kwargs['socket']
    else:
      self.socket_type = socket

    self.pool = redis.ConnectionPool(host=self.host,
                                     port=self.port,
                                     db=self.db)

  def instantiate(self, async=True):
    r = redis.Redis(connection_pool=self.pool)
    return r


class Logger(object):
  """
  Creates a logger that uses Redis as a processing
  pipe. This means that multiple processes can log
  to a single log 'key'.

  Intended to be used with a LogSink that will dump
  the log output to a file
  """
  ERROR = 1
  WARN = 2
  INFO = 3
  DEBUG = 4

  def __init__(self, rinfo, log_key, entity_id, level):

    self.level = level

    try:
      self._r = rinfo.instantiate(async=False)
    except AttributeError:
      if isinstance(rinfo, redis.client.Redis):
        self._r = rinfo
      else:
        raise TypeError("Expected either ConnectionInfo or "
                        "RedisConnection object")

    if not self._r.ping():
      raise redis.ConnectionError(
          "Couldn't connect logger to Redis backend")

    self.log_key = "logger:{0}".format(log_key)
    self.entity_id = entity_id

  def _log(self, level, msg):
    fmt_msg = "{0}:{1}:{2}:: {3}".format(
        time.time(),
        self.entity_id,
        level,
        msg)

    self._r.lpush(self.log_key, fmt_msg)

  def debug(self, msg):
    if self.level >= Logger.DEBUG:
      self._log("DEBUG", msg)

  def info(self, msg):
    if self.level >= Logger.INFO:
      self._log("INFO", msg)

  def warn(self, msg):
    if self.level >= Logger.WARN:
      self._log("WARNING", msg)

  def error(self, msg):
    if self.level >= Logger.ERROR:
      self._log("ERROR", msg)


class LogSink(multiprocessing.Process):
  """
  Act as a sink for a set of redis.Loggers
  by reading from the log buffer continuously
  and writing the output to a file.
  """

  def __init__(self, ident, r_info, log_keys, sinkfile):

    self.identifier = ident
    log_keys = ["logger:{0}".format(x) for x in log_keys]
    multiprocessing.Process.__init__(self, target=self._worker,
                                     args=[sinkfile,
                                           r_info,
                                           log_keys])

    self._r = redis.StrictRedis(r_info.host, r_info.port, r_info.db)
    if not self._r.set("logsink:{0}:operate".format(ident), 1):
      raise redis.ConnectionError(
          "Couldn't connect logger to Redis backend")
    self._r.delete(*log_keys)

    if not isinstance(log_keys, list):
      raise TypeError("Expected list of log keys")

    try:
      tstfile = open(sinkfile, 'w')
    except:
      raise IOError("Can't write to sinkfile {0}".format(sinkfile))
    finally:
      tstfile.close()

  def shutdown(self):
    self._r.delete("logsink:{0}:operate".format(self.identifier))

  def _worker(self, sinkfile, redis_info, log_keys):
    r = redis.StrictRedis(redis_info.host,
                          redis_info.port,
                          redis_info.db)

    fout = open(sinkfile, 'w')
    fout.write("Logsink {0} started up OK\n".format(self.ident))
    fout.flush()

    i = 0
    while True:
      result = r.brpop(*log_keys, timeout=2)

      if result:
        fout.write("{0}:{1}\n".format(*result))
        i += 1
        if i > 5:
          fout.flush()
          i = 0
      else:
        fout.flush()
        i = 0
        if not r.exists("logsink:{0}:operate".format(self.identifier)):
          fout.close()
          return
      result = None


class RedisMutex:
    def __init__(self, redis, name):
        self._r = redis
        self._name = name
        if self._r.getset('mutex:%s:init' % self._name, 1) != '1':
            self._r.rpush("mutex:%s" % self._name, 1)
            self._r.ltrim("mutex:%s" % self._name, 0, 0)
        self.locked_by_us = False

    def owned(self):
      return self.locked_by_us

    def acquire(self, silent=False):
        if self.locked_by_us:
          log.warn("Error: deadlock in mutex acquire")
          raise Exception("Tried to acquire mutex we already hold")

        self._r.brpop('mutex:%s' % self._name, timeout=0)
        self.locked_by_us = True

    def release(self):
        self._r.rpush('mutex:%s' % self._name, 1)
        self.locked_by_us = False

    def is_locked(self):
        if self._r.llen('mutex:%s' % self._name) == 0:
            return True
        return False

    def wait(self):
        self.acquire(silent=True)
        self.release()

    def backend(self):
        return self._r
