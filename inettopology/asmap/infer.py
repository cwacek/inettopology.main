import bdb
import subprocess
import sys
import json
import os
import time
import signal

from inettopology import SilentExit
import inettopology.asmap.util as utils
import inettopology.asmap.util.structures as redis_structures

CHECK_VF_SCRIPT = None
wait_queue = None
GREENLETS = dict()


def add_cmdline_args(subp, parents):
  """Add commandline arguments for this module
  to the subparser :subp:.

  Include :parents: as parents of the parser.

  :subp: argparse.SubParser
  :parents: list of argparse.Parsers
  """

  infer_parser = subp.add_parser("infer",
                                 help="Infer AS level paths",
                                 parents=parents)
  infer_parser.add_argument("--log", help="where to log activity to")
  infer_parser.add_argument("--tags",
                            help="The RIB tags to include above the base",
                            nargs='+', required=True)
  infer_parser.add_argument("--inferrer_count", "-c",
                            help="The number of inferrers per tag",
                            default=1, type=int)
  infer_parser.add_argument("--inferrer_bin",
                            help="The binary to use for inference.",
                            default="./as_infer")
  infer_parser.add_argument("--include-ixps",
                            help="Include notes about IXPs from this datafile",
                            metavar="IXP_DATAFILE")
  infer_parser.add_argument("--translate-ips",
                            help="Include the capability to translate IPs "
                                 "using a MaxMind GeoIP database",
                            metavar="GEOIP_DB")
  existing_elems = infer_parser.add_mutually_exclusive_group()
  existing_elems.add_argument("--force",
                              help="Leave existing elements in the queue",
                              action="store_true")
  existing_elems.add_argument("--reset",
                              help="Clear the queue before starting",
                              action="store_true")
  infer_parser.set_defaults(func=_gao_inference_helper)


def _gao_inference_helper(args):
  """ A helper to allow not importing gao_inference unless
  it's being run. gao_inference uses gevent, and that means
  no PyPy"""
  import gevent
  import gevent.socket
  from gevent import monkey
  monkey.patch_all()

  start_inference_service(args)


class SocketTimeout(Exception):
  pass


class RequestHelper(object):

  @staticmethod
  def err_resp_obj(msg):
    return {'type': 'error', 'msg': "{0}".format(msg)}

  @staticmethod
  def err_resp(msg):
    return json.dumps({'type': 'error', 'msg': "{0}".format(msg)})

  @staticmethod
  def resp_obj(tag, src, dst, path):
    return {'type': 'response', 'tag': tag,
            'src': src, 'dst': dst, 'path': path, 'ixps': {}}


class TranslationError(Exception):
  def __init__(self, ip):
    self.ip = ip

  def msg(self):
    return "Failed to translate '{0}' to AS".format(self.ip)


class InferenceGreenletServer(object):
  """ A greenlet servers which listens for
  inference requests and has :handler: handle each one
  that arrives.
  """

  _s = None

  def __init__(self, server_address, handler):
    self._s = gevent.socket.socket()
    self._s.bind(('0.0.0.0', 9323))

  def start(self):
    self._s.listen(500)
    while True:
      cli, addr = self._s.accept()
      greenlet = gevent.spawn(greenlet_handle, cli, self)
      GREENLETS[id(greenlet)] = greenlet

  def shutdown(self):
    gevent.killall(GREENLETS.itervalues())

  def translate_addresses(self, request):

    self.log.debug("Translating {0}".format(request))

    if request['src'][1] == 'IP':
      try:
        src_as = self.geoipdata.org_by_addr(request['src'][0])
      except:
        self.log.warn("Failed to lookup AS for request")
        raise TranslationError(request['src'])

      try:
        request['src'] = src_as.split()[0].strip("AS")
      except:
        self.log.warn("Failed to translate source'{0}' "
                      "from IP to AS for request {1}"
                      .format(request['src'], request))
        raise TranslationError(request['src'])
    elif request['src'][1] == 'AS':
      request['src'] = request['src'][0]
    else:
      return None

    if request['dst'][1] == 'IP':
      try:
        dst_as = self.geoipdata.org_by_addr(request['dst'][0])
      except:
        self.log.warn("Failed to lookup AS for request")
        raise TranslationError(request['dst'])
      try:
        request['dst'] = dst_as.split()[0].strip("AS")
      except:
        self.log.warn("Failed to translate destination '{0}' "
                      "from IP to AS for request {1}"
                      .format(request['dst'], request))
        raise TranslationError(request['src'])
    elif request['src'][1] == 'AS':
      request['dst'] = request['dst'][0]
    else:
      return None

    #self.log.info("Translated to {0}".format(request))
    return request


class ProcessingEventQueue(object):
  """ Track events which have been requested
  for processing
  """

  def __init__(self, logger):
    self.events = dict()
    self.num_waiting = 0
    self.log = logger

  def register_event(self, event_tag, event):
    """
    Register an event against the event_tag
    tag, to be notified when event_tag is
    finished processing.

    returns False if there was already someone
    waiting for event_tag, True otherwise
    """
    self.num_waiting += 1
    try:
      self.events[event_tag].append(event)
      return False
    except:
      self.events[event_tag] = [event]
      return True

  def log_status(self):
    self.log.info("ProcessingEventQueue: Have {0} handlers "
                  "waiting on {1} events"
                  .format(self.num_waiting, len(self.events)))

  def fire(self, event_tag):
    """
    Fire the event handlers for everything
    registered to event_tag
    """
    if event_tag not in self.events:
      self.log.info("Asked to fire events for {0}, "
                    "which has no listeners".format(event_tag))
      return

    self.log.debug("Firing events for {0} listeners of {1}"
                   .format(len(self.events[event_tag]), event_tag))
    for event in self.events[event_tag]:
      event.set()
      self.num_waiting -= 1

    del self.events[event_tag]


def start_inference_service(args):
  """
  Start up an inference service for AS Paths.
  """

  redis_info = redis_structures.ConnectionInfo(**args.redis)
  log_rinfo = redis_structures.ConnectionInfo(**args.redis)
  r = redis_info.instantiate()
  r.ping()
  r.ping()
  r.set("testblashdls", "sinweljkrfs")
  logsink = redis_structures.LogSink('route_inference',
                                     log_rinfo,
                                     ["route_inference"],
                                     args.log)
  try:
    logsink.start()

    log = redis_structures.Logger(redis_info,
                                  'route_inference',
                                  "controller",
                                  redis_structures.Logger.INFO)

    tag_inferrers = dict()

    if args.include_ixps:
      ixpdata = dict()
      try:
        with open(args.include_ixps) as fin:
          for line in fin:
            ixp, as1, as2, confidence = line.strip().split(None, 3)
            ixpdata[(as1, as2)] = (ixp, confidence)
      except IOError, e:
        raise Exception("Failed to open IXP datafile [{0}]".format(e))

      log.info("loaded IXP Datafile with {0} IXP crossings"
               .format(len(ixpdata)))

    if args.translate_ips:
      log.info("Loading GeoIP database.")
      try:
        import pygeoip
        geoipdata = pygeoip.GeoIP(args.translate_ips, pygeoip.MEMORY_CACHE)
      except IOError, e:
        raise Exception("Failed to open GeoIP database [{0}]".format(e))
      except ImportError:
        raise Exception("IP Translation requires the pygeoip library: "
                        "'pip install pygeoip'")

    for tag in args.tags:
      pq = redis_structures.ProcessingQueue(r,
                                            "{0}_procqueue".format(tag))
      if len(pq) > 0:

        log.info("There are {0} elements in the processing queue for {1}. "
                 .format(len(pq), tag))
        if args.force:
          log.info("Continuing anyway.")
        elif args.reset:
          log.info("Clearing processing queue")
          pq.reset()
        else:
          log.info("\nRun with --force to leave them there, or "
                   "--reset to clear them out")
          raise SilentExit()
      for i in xrange(args.inferrer_count):
        inf = _start_inferrer(args.inferrer_bin, tag)
        try:
          tag_inferrers[tag].append(inf)
        except KeyError:
          tag_inferrers[tag] = [inf]

    server = InferenceGreenletServer(('0.0.0.0', 9323), greenlet_handle)
    #server = InferenceServer(('0.0.0.0', 9323), RequestHelper)
    server.r = r
    server.redis_info = redis_info
    server.ixpdata = ixpdata
    server.geoipdata = geoipdata
    server.log = log

    result_watcher_gr = gevent.spawn(watch_query_results, server)
    GREENLETS[id(result_watcher_gr)] = result_watcher_gr

    log.info("Starting server listening on 9323")
    try:
      server.start()
      server.join()
    except (KeyboardInterrupt, bdb.BdbQuit) as e:
      server.shutdown()
      raise

    #try:
      #server.serve_forever()
      #server.join()
    #except (KeyboardInterrupt, bdb.BdbQuit) as e:
      #server.shutdown()
      #raise

  except OSError as e:
    log.error("Error launching {1}: {0}\n"
              .format(e, args.inferrer_bin))

  except (KeyboardInterrupt, bdb.BdbQuit) as e:
    pass
  except (Exception) as e:
    log.error("Error: {0}\n".format(e))
  finally:
    for inferrer_list in tag_inferrers.itervalues():
      for inferrer in inferrer_list:
        inferrer.terminate()
    logsink.shutdown()
    if logsink.is_alive():
      sys.stderr.write("Giving logsink 5 seconds to exit\n")
      time.sleep(5)
    os.kill(logsink.pid, signal.SIGKILL)


def _start_inferrer(infer_proc, ribtag):
  """ Start the inference binary :infer_proc: and
  ask it to infer for :ribtag:
  """

  cmd = [infer_proc, "-r", ribtag,
         "--procqueue", "{0}_procqueue".format(ribtag)]
  sys.stderr.write("Starting inferrer as '{0}'\n".format(" ".join(cmd)))
  pid = subprocess.Popen(cmd,
                         stderr=open("{0}_inferrer.log".format(ribtag), 'w'))

  if pid.poll() is not None:
    sys.stderr.write("Inferrer for {0} failed. stderr: '{0}'"
                     .format(ribtag, pid.stderr.read()))
    return None

  return pid


def greenlet_handle(sock, server):
  """ Handle a request received by :server:
    on :sock: """

  log = redis_structures.Logger(server.r,
                                'route_inference',
                                "handler_{0}".format(id(gevent.getcurrent())),
                                redis_structures.Logger.INFO)

  try:
    try:
      gevent.socket.wait_read(sock.fileno(),
                              timeout=10,
                              timeout_exc=SocketTimeout())
    except SocketTimeout as e:
      log.info("Closing timed out socket\n")
      sock.close()
      return

    res = sock.recv(1024)
    if not res:
      log.warn("Socket Error\n")
      sock.close()
      return

    data = res.strip()
    try:
      req = json.loads(data)
    except ValueError:
      sys.stderr.write("Received unparseable request: '{0}'\n"
                       .format(data))
      return sock.sendall(RequestHelper.err_resp("Unparseable"))

    if 'type' not in req or req['type'] != 'request':
      sys.stderr.write("Received malformed request: '{0}'\n"
                       .format(data))
      return sock.sendall(RequestHelper.err_resp("Malformed"))

    try:
      req = server.translate_addresses(req)
    except TranslationError as e:
      return sock.sendall(RequestHelper.err_resp(e.msg()))

    if not req:
      sys.stderr.write("Received malformed request: '{0}'\n"
                       .format(data))
      return sock.sendall(RequestHelper.err_resp("Malformed Types"))

    resp = mk_inference_request(server, log,
                                req['tag'], req['src'], req['dst'])

    if server.ixpdata and resp['type'] != 'error' and resp['path']:
      for as1, as2 in utils.pairwise(resp['path'].split()):
        try:
          ixp = server.ixpdata[(as1, as2)]
          resp['ixps'][ixp[0]] = {'as1': as1, 'as2': as2, 'confidence': ixp[1]}
        except KeyError, e:
          pass

    log.debug("Response: '{0}'".format(resp))
    sock.sendall(json.dumps(resp))

    del GREENLETS[id(gevent.getcurrent())]

  except gevent.GreenletExit:
    sock.close()
    log.info("socket_handler exiting")
    return
  except Exception as e:
    sock.close()
    import traceback
    log.info("ERROR {0}\n".format(traceback.format_exception(*sys.exc_info())))
    return


def watch_query_results(server):
  """ Inference queries are performed via callbacks.
  Listen for notification that an inference has
  completed, then allow the handler to respond to the
  original request
  """

  global wait_queue
  wait_queue = ProcessingEventQueue(server.log)
  try:
    listener = server.r.pubsub()
    listener.subscribe(['inference:query_status'])

    for item in listener.listen():
      if item['type'] == 'message':
        wait_queue.fire(item['data'])
        wait_queue.log_status()
  except gevent.GreenletExit:
    server.log.info("query_watcher exiting")
    return


def mk_inference_request(server, log, ribtag, as1, as2):
  """ Make an inference request to one of the
  inferrers for the path between :as1: and :as2:
  tagged by :ribtag:.
  """

  # Processing all sources to one destination, so
  # the event tag is the ribtag plus the destination.
  event_tag = "{0}|{1}".format(ribtag, as2)
  global wait_queue

  path = server.r.hget("result:{0}:inferred_to:{1}"
                       .format(ribtag, as2), as1)

  if path:
    return RequestHelper.resp_obj(ribtag, as1, as2, path)

  # If None was returned, but the key is in the database,
  # then there is no known path.
  searched = server.r.exists("result:{0}:inferred_to:{1}".format(ribtag, as2))
  if not path and searched:
    return RequestHelper.resp_obj(ribtag, as1, as2, None)

  # Check if we already requested that someone process this
  # instead of asking again
  wait_for = gevent.event.Event()
  wait_for.clear()

  if wait_queue.register_event(event_tag, wait_for) is True:
    # True means we're the only one in the event, so we better actually
    # schedule processing.

    log.debug("Requesting computation of {1} from {0}_procqueue"
              .format(event_tag, ribtag))

    procqueue = redis_structures.ProcessingQueue(
        server.r,
        "{0}_procqueue".format(ribtag),
        track_seen=False)
    if not procqueue.has_listeners():
      log.debug("There is no handler for {0}.".format(ribtag))
      return RequestHelper.err_resp_obj("No handler exists for tag '{0}'"
                                        .format(ribtag))

    procqueue.add(as2)
  else:
    log.debug("Computation for {0} already requested. Waiting for result"
              .format(event_tag))

  wait_for.wait(180)
  log.debug("Got inferrer response with tag {0}".format(event_tag))

  if not wait_for.isSet():
    return RequestHelper.err_resp_obj("Inference server didn't respond "
                                      "in 180 seconds")

  path = server.r.hget("result:{0}:inferred_to:{1}"
                       .format(ribtag, as2), as1)

  return RequestHelper.resp_obj(ribtag, as1, as2, path)
