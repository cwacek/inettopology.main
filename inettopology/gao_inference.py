import traceback
import multiprocessing
import os
import signal
import sys
import subprocess
import json
import SocketServer
import bdb
import itertools
import time
import as_graph
import ast
import redis_structures

import gevent
import gevent.socket
from gevent import monkey
monkey.patch_all()

def INFERRED(dest,tags):
  return "inferred_to:{0}:tags:{1}".format(dest,"_".join(tags))

INFERRED_KEYS = "inferred:keylist"

CHECK_VF_SCRIPT = None

class SocketTimeout(Exception):
  pass

class TranslationError(Exception):
  def __init__(self,ip):
    self.ip = ip

  def msg(self):
    return "Failed to translate '{0}' to AS".format(self.ip)

class ProcessingEventQueue(object):
  def __init__(self,logger):
    self.events = dict()
    self.num_waiting = 0
    self.log = logger

  def register_event(self,event_tag,event):
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
    self.log.info("ProcessingEventQueue: Have {0} handlers waiting on {1} events"
              .format(self.num_waiting, len(self.events)))

  def fire(self,event_tag):
    """
    Fire the event handlers for everything 
    registered to event_tag
    """
    if event_tag not in self.events:
      self.log.info("Asked to fire events for {0}, which has no listeners".format(event_tag))
      return

    self.log.debug("Firing events for {0} listeners of {1}"
                    .format(len(self.events[event_tag]),event_tag))
    for event in self.events[event_tag]:
      event.set()
      self.num_waiting -= 1

    del self.events[event_tag]

wait_queue = None
GREENLETS = dict()

def watch_query_results(server):
  global wait_queue
  wait_queue = ProcessingEventQueue(server.log)
  try:
    listener = server.r.pubsub()
    listener.subscribe( ['inference:query_status'] )

    for item in listener.listen():
      if item['type'] == 'message':
        wait_queue.fire(item['data'])
        wait_queue.log_status()
  except gevent.GreenletExit:
    server.log.info("query_watcher exiting")
    return

def mk_inference_request(server,log,ribtag,as1,as2):
# This is the tag of the processing we need 
# for this request
  event_tag = "{0}|{1}".format(ribtag,as2)
  global wait_queue

  path = server.r.hget("result:{0}:inferred_to:{1}"
                    .format(ribtag,as2),as1)

  if path:
    return {'type': 'response', 'tag':ribtag,
            'src': as1, 'dst': as2, 'path': path, 'ixps': {}}

  searched = server.r.exists("result:{0}:inferred_to:{1}".format(ribtag,as2))
  if not path and searched:
    return {'type': 'response', 'tag':ribtag,
            'src': as1, 'dst': as2, 'path': None, 'ixps': {}}

# Check if we already requested that someone process this 
# instead of asking again
  wait_for = gevent.event.Event()
  wait_for.clear()

  if wait_queue.register_event(event_tag,wait_for) is True:
    # True means we're the only one in the event, so we better actually
    # schedule processing.

    log.debug("Requesting computation of {1} from {0}_procqueue"
                    .format(ribtag,event_tag))
    procqueue = redis_structures.ProcessingQueue(server.r,
                                "{0}_procqueue".format(ribtag),
                                track_seen=False)
    if not procqueue.has_listeners():
      log.debug("There is no handler for {0}.".format(ribtag))
      return {'type': 'error',
              'msg': "No handler exists for tag '{0}'".format(ribtag)}

    procqueue.add(as2);
  else:
    log.debug("Computation for {0} already requested. Waiting for result".format(event_tag))

  wait_for.wait(180)
  log.debug("Got inferrer response with tag {0}".format(event_tag))

  if not wait_for.isSet():
    return ASPathRequestHandler.err_resp_obj("Inference server didn't respond in 180 seconds")

  path = server.r.hget("result:{0}:inferred_to:{1}"
                    .format(ribtag,as2),as1)

  return {'type': 'response', 'tag':ribtag,
          'src': as1, 'dst': as2, 'path': path, 'ixps': {}}

def greenlet_handle(sock,server):
  log = redis_structures.Logger(server.r,'route_inference',"handler_{0}"
                                          .format(id(gevent.getcurrent())),redis_structures.Logger.INFO)

  try:
    try:
      gevent.socket.wait_read(sock.fileno(),timeout=10,timeout_exc=SocketTimeout())
    except SocketTimeout as e:
      log.info("Closing timed out socket\n")
      sock.close()
      return

    res = sock.recv(1024)
    if not res:
      sys.stderr.write("Socket Error\n")
      sock.close()
      return

    data = res.strip()
    try:
      req = json.loads(data)
    except ValueError:
      sys.stderr.write("Received unparseable request: '{0}'\n"
                       .format(data))
      return sock.sendall(ASPathRequestHandler.err_resp("Unparseable"))

    if 'type' not in req or req['type'] != 'request':
      sys.stderr.write("Received malformed request: '{0}'\n"
                       .format(data))
      return sock.sendall(ASPathRequestHandler.err_resp("Malformed"))

    try:
      req = server.translate_addresses(req)
    except TranslationError as e:
      return sock.sendall(ASPathRequestHandler.err_resp(e.msg()))

    if not req:
      sys.stderr.write("Received malformed request: '{0}'\n"
                       .format(data))
      return sock.sendall(ASPathRequestHandler.err_resp("Malformed Types"))

    #log.info("Request: '{0}'".format(req))
    resp = mk_inference_request(server,log,req['tag'],req['src'],req['dst'])

    if server.ixpdata and resp['type'] != 'error' and resp['path']:
      for as1,as2 in pairwise(resp['path'].split()):
        try:
          ixp = server.ixpdata[(as1,as2)]
          resp['ixps'][ixp[0]] = {'as1': as1, 'as2':as2, 'confidence':ixp[1]}
        except KeyError, e:
          pass

    log.debug("Response: '{0}'".format(resp))
    sock.sendall(json.dumps(resp))

    del GREENLETS[ id(gevent.getcurrent()) ]

  except gevent.GreenletExit:
    sock.close()
    log.info("socket_handler exiting")
    return
  except Exception as e:
    sock.close()

    log.info("ERROR {0}\n".format(traceback.format_exception(*sys.exc_info())))
    return

class ASPathRequestHandler(SocketServer.BaseRequestHandler):

  @staticmethod
  def err_resp_obj(msg):
    return {'type': 'error', 'msg': "{0}".format(msg)}

  @staticmethod
  def err_resp(msg):
    return json.dumps({'type': 'error', 'msg': "{0}".format(msg)})

  def handle(self):
    res = self.request.recv(1024)
    if not res:
      sys.stderr.write("Socket Error")
      return

    data = res.strip()
    try:
      req = json.loads(data)
    except ValueError:
      sys.stderr.write("Received unparseable request: '{0}'\n"
                       .format(data))
      return self.request.sendall(self.err_resp("Unparseable"))

    if 'type' not in req or req['type'] != 'request':
      sys.stderr.write("Received malformed request: '{0}'\n"
                       .format(data))
      return self.request.sendall(self.err_resp("Malformed"))

    req = self.server.translate_addresses(req)
    if not req:
      sys.stderr.write("Received malformed request: '{0}'\n"
                       .format(data))
      return self.request.sendall(self.err_resp("Malformed Types"))

    self.server.log.debug("Received request: '{0}'".format(req))
    resp = get(self.server,req['tag'],req['src'],req['dst'])

    self.server.log.debug("Have IXP data? {0}".format(True if self.server.ixpdata else False))
    if self.server.ixpdata and resp['type'] != 'error' and resp['path']:
      for as1,as2 in pairwise(resp['path'].split()):
        try:
          ixp = self.server.ixpdata[(as1,as2)]
          resp['ixps'][ixp[0]] = {'as1': as1, 'as2':as2, 'confidence':ixp[1]}
        except KeyError, e:
          pass

    self.server.log.debug("Sending response: '{0}'".format(resp))
    self.request.sendall(json.dumps(resp))

class InferenceGreenletServer(object):

  _s = None

  def __init__(self,server_address,handler):
    self._s = gevent.socket.socket()
    self._s.bind(('0.0.0.0',9323))

  def start(self):
    self._s.listen(500)
    while True:
      cli, addr = self._s.accept()
      greenlet = gevent.spawn(greenlet_handle,cli,self)
      GREENLETS[id(greenlet)] = greenlet

  def shutdown(self):
    gevent.killall(GREENLETS.itervalues())

  def translate_addresses(self,request):

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
                      "from IP to AS for request {1}".format(request['src'],request))
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
                      "from IP to AS for request {1}".format(request['dst'],request))
        raise TranslationError(request['src'])
    elif request['src'][1] == 'AS':
      request['dst'] = request['dst'][0]
    else:
      return None

    #self.log.info("Translated to {0}".format(request))
    return request

class InferenceServer(SocketServer.TCPServer):
#class InferenceServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):

  allow_reuse_address = True
  daemon_threads = True

  def __init__(self,server_address,RequestHandlerCls):
    SocketServer.TCPServer.__init__(self,server_address,RequestHandlerCls)

  def translate_addresses(self,request):

    self.log.info("Translating {0}".format(request))

    if request['src'][1] == 'IP':
      src_as = self.geoipdata.org_by_addr(request['src'][0])
      request['src'] = src_as.split()[0].strip("AS")
    elif request['src'][1] == 'AS':
      request['src'] = request['src'][0]
    else:
      return None

    if request['dst'][1] == 'IP':
      dst_as = self.geoipdata.org_by_addr(request['dst'][0])
      request['dst'] = dst_as.split()[0].strip("AS")
    elif request['src'][1] == 'AS':
      request['dst'] = request['dst'][0]
    else:
      return None

    self.log.info("Translated to {0}".format(request))
    return request

def start_inference_service(args):
  """
  Start up an inference service for AS Paths.
  """

  redis_info = redis_structures.ConnectionInfo(**args.redis)
  log_rinfo = redis_structures.ConnectionInfo(**args.redis)
  r= redis_info.instantiate()
  r.ping()
  r.ping()
  r.set("testblashdls", "sinweljkrfs")
  logsink = redis_structures.LogSink('route_inference',
                                     log_rinfo,
                                     ["route_inference"],
                                     args.log)
  try:
    logsink.start()

    log = redis_structures.Logger(redis_info,'route_inference',"controller",redis_structures.Logger.INFO)

    tag_inferrers = dict()

    if args.include_ixps:
      ixpdata = dict()
      try:
        with open(args.include_ixps) as fin:
          for line in fin:
            ixp,as1,as2,confidence = line.strip().split(None,3)
            ixpdata[(as1,as2)] = (ixp,confidence)
      except IOError, e:
        raise Exception("Failed to open IXP datafile [{0}]".format(e))
      log.info("loaded IXP Datafile with {0} IXP crossings".format(len(ixpdata)))

    if args.translate_ips:
      log.info("Loading GeoIP database.")
      try:
        import pygeoip
        geoipdata = pygeoip.GeoIP(args.translate_ips,pygeoip.MEMORY_CACHE)
      except IOError, e:
        raise Exception("Failed to open GeoIP database [{0}]".format(e))
      except ImportError:
        raise Exception("IP Translation requires the pygeoip library: 'pip install pygeoip'")

    for tag in args.tags:
      pq = redis_structures.ProcessingQueue(r,
                              "{0}_procqueue".format(tag))
      if len(pq) > 0:
        sys.stderr.write("There are {0} elements in the processing queue for {1}. "
                         .format(len(pq),tag))
        if args.force:
          sys.stderr.write("Continuing anyway.\n")
        elif args.reset:
          sys.stderr.write("Clearing processing queue\n")
          pq.reset()
        else:
          sys.stderr.write("\nRun with --force to leave them there, or --reset to clear them out\n")
          sys.exit(1)
      for i in xrange(args.inferrer_count):
        inf = _start_inferrer(args.inferrer_bin,tag)
        try:
          tag_inferrers[tag].append(inf)
        except KeyError:
          tag_inferrers[tag] = [inf]

    server = InferenceGreenletServer(('0.0.0.0',9323),greenlet_handle)
    #server = InferenceServer(('0.0.0.0',9323),ASPathRequestHandler)
    server.r = r
    server.redis_info = redis_info
    server.ixpdata = ixpdata
    server.geoipdata = geoipdata
    server.log = log

    result_watcher_gr = gevent.spawn(watch_query_results,server)
    GREENLETS[id(result_watcher_gr)] =result_watcher_gr

    log.info("Starting server listening on 9323");
    try:
      server.start()
      server.join()
    except (KeyboardInterrupt,bdb.BdbQuit) as e:
      server.shutdown()
      raise

    #try:
      #server.serve_forever()
      #server.join()
    #except (KeyboardInterrupt,bdb.BdbQuit) as e:
      #server.shutdown()
      #raise

  except OSError as e:
    sys.stderr.write("Error launching {1}: {0}\n"
                      .format(e, args.inferrer_bin))

  except (KeyboardInterrupt,bdb.BdbQuit) as e:
    pass
  except (Exception) as e:
    sys.stderr.write("Error: {0}\n".format(e))
  finally:
    for inferrer_list in tag_inferrers.itervalues():
      for inferrer in inferrer_list:
        inferrer.terminate()
    logsink.shutdown()
    if logsink.is_alive():
      sys.stderr.write("Giving logsink 5 seconds to exit\n")
      time.sleep(5)
    os.kill(logsink.pid,signal.SIGKILL)

def _start_inferrer(infer_proc, ribtag):

  cmd = [infer_proc, "-r",ribtag, "--procqueue","{0}_procqueue".format(ribtag)]
  sys.stderr.write("Starting inferrer as '{0}'\n".format(" ".join(cmd)))
  pid = subprocess.Popen(cmd,stderr=open("{0}_inferrer.log".format(ribtag),'w'))

  if pid.poll() is not None:
    sys.stderr.write("Inferrer for {0} failed. stderr: '{0}'"
                      .format(ribtag,pid.stderr.read()))
    return None

  return pid


def infer_routes(args):
  """
  Use Gao's inference algorithm to infer routes on a composite
  AS graph.

  The composite graph is constructed by combining the base AS
  set with the ones labeled with the tags in rib_tags. By
  definition, only the rib_tags have sure_paths, so that's why
  install those into the graph.
  """

  return start_inference_service(args)

  redis_info = redis_structures.ConnectionInfo(**args.redis)
  r= redis_info.instantiate()

  # Initialize our valley-free checker so we only have to do it once
  global CHECK_VF_SCRIPT
  CHECK_VF_SCRIPT = r.register_script(Path.VF_LUA)

  # Set up our multiprocessing logger
  logsink = redis_structures.LogSink('route_inference',
                                     redis_info,
                                     ["route_inference"],
                                     args.log)
  try:
    logsink.start()
    log = redis_structures.Logger(redis_info,'route_inference',"controller",redis_structures.Logger.INFO)

    as_rel_keys = redis_structures.Collection(r,as_graph.AS_REL_KEYS)
    inferred_path_tags = redis_structures.KeyedCollection(r,INFERRED_KEYS)
    inferred_path_tags.delete(" ".join(args.tags))

    if len(as_rel_keys) < 1:
      log.error("Cannot perform route inference without AS relationships. "
                "Run the 'load --asrels' command")
      return False

    log.info("Building the set of all destinations we need to reach")

    as_set = redis_structures.Collection(r,as_graph.BASE_ASES)
    all_ases = as_set.members()

    for tag in args.tags:
      tagged_set = redis_structures.Collection(r,'{0}_ases'.format(tag))
      all_ases |= tagged_set.members()

    destination_queue = redis_structures.ProcessingQueue(r,"destination_queue")
    destination_queue.reset()
    destination_queue.add_from(all_ases)
    log.info("Full set of destinations identified")


    start_time = time.time()

    log.info("{0} destinations remaining to process".format(len(all_ases)))

    raw_input()
    #workers = []
    #for i in xrange(10):
      #workers.append(KnownPathWorker(redis_info,'destination_queue',args.tags))
      #workers[i].start()
      #log.info("Starting worker!")

    #for worker in workers:
      #worker.join()

    log.info("Inferring routes for {0} took {1} seconds".format(args.tags,time.time()-start_time))
    logsink.shutdown()
    logsink.join()

  except (KeyboardInterrupt,bdb.BdbQuit):
    logsink.shutdown()
    logsink.join()


class KnownPathWorker(multiprocessing.Process):

  def __init__(self,redis_info,dest,rib_tags):

    multiprocessing.Process.__init__(self,
                                     target=self.known_path,
                                     args=[redis_info,
                                           dest,
                                           rib_tags])

  def known_path(self,redis_info,dest,rib_tags):

    r = redis_info.instantiate()
    log = redis_structures.Logger(redis_info,'route_inference',self.name,redis_structures.Logger.INFO)

    destination_queue = redis_structures.ProcessingQueue(r,dest)
    inferred_path_tags = redis_structures.KeyedCollection(r,INFERRED_KEYS)

    # We're going to pre-load a ton of data to try and speed this up.
    # We do it outside of the processing loop to try and make things
    # done once
    as_set = redis_structures.Collection(r,as_graph.BASE_ASES)
    all_ases = as_set.members()

    for tag in rib_tags:
      tagged_set = redis_structures.Collection(r,'{0}_ases'.format(tag))
      all_ases |= tagged_set.members()
    """ all_ases is all ases in the db """

    # Now we load link data into a native data type for speed
    as_rel_keys = redis_structures.Collection(r,as_graph.AS_REL_KEYS)
    base_as_links = redis_structures.KeyedCollection(r,as_graph.BASE_LINKS)
    log.info("Loading Link Structure Data")
    as_links = dict()
    for i,AS in enumerate(all_ases):
      as_links[AS] = dict()
      peers = []

      # We're going ot try and do this by pipelining as
      # much as humanly possible
      pipe = r.pipeline(transaction=False)
      for peer in base_as_links.members(AS):
        peers.append(peer)
        pipe.hget(as_graph.AS_REL(AS),peer)
      result = pipe.execute()

      linkdict = dict(zip(peers,({'rel':x } for x in result)))
      as_links[AS].update(linkdict)

      for tag in rib_tags:
        rib_links = redis_structures.KeyedCollection(
                                      r,
                                      as_graph.TAG_LINKS(tag))
        peers=[]
        pipe = r.pipeline(transaction=False)
        for peer in base_as_links.members(AS):
          peers.append(peer)
          pipe.hget(as_graph.AS_REL(AS),peer)
        result = pipe.execute()

        linkdict = dict(zip(peers,({'rel':x } for x in result)))
        as_links[AS].update(linkdict)
      log.info("Loaded links for {0}/{1} ASes".format(i+1,len(all_ases)))

    missing_rel_data = set() #for tracking
    log.info("Link Structure loading done")

    while True:
      dest_timer = time.time()
      dest = destination_queue.get_next()
      if dest is None:
        break

      queue = redis_structures.ProcessingQueue(redis_info,'gao_infer_queue:{0}'.format(dest))

      # Step 1: Load the right ASes into the graph we're going to search on
      log.info("Building Queue to infer routes to {0}".format(dest))
      base_ases,rib_in = self.init_active_queue(r,queue,dest,rib_tags,log)

      if len(queue) == 0:
        log.warn("No known routes to {0}".format(dest))

      while queue:
        u = queue.get_next()
        #log.info("Pulled {0} from queue.".format(u))

        # We need to find all the peers for this element
        # This means we're going to search through the
        # base_as_links, and then through all links designated in
        # our RIBs
        #start = time.time()
        u_peers = as_links[u]

        for v in u_peers:

          # We skip the ones belonging to the base AS
          if v in base_ases:
            continue

          best_candidate = rib_in.peek(u,copy=True)
          best_candidate.prepend(v)

          # Check for loops
          if not best_candidate.valid:
            #log.debug("Removed candidate path {0} with loops "
                        #.format(best_candidate))
            continue

          # Check if we're okay with valley freeness
          vf_result = best_candidate.check_valley_free(as_links)
          if vf_result[0] is False:
            #msg = "Removed candidate path {0}. ".format(best_candidate)
            if vf_result[1]:
              missing_rel_data.add(vf_result[1])
              #msg += "Due to MISSING DATA."
            #log.debug(msg)
            continue

          # See if the peer's path changed
          tmp_path = rib_in.peek(v)
          rib_in.add(v,best_candidate)
          if tmp_path != rib_in.peek(v):
            #log.debug("{0} got a new potential best path. Adding to queue"
                     #.format(v))
            queue.add(v)

      pipe = r.pipeline(transaction=False)
      for origin in rib_in:
        pipe.hset(INFERRED(dest,rib_tags),origin,rib_in.peek(origin))
        inferred_path_tags.add("_".join(rib_tags),
                               [INFERRED(dest,rib_tags)],
                               pipe=pipe)
      pipe.execute()
      log.info("Done. Inferred routes to {0}. Took {1} seconds."
               .format(dest,time.time() - dest_timer))
      log.warn("Missing {0} AS relationships that were otherwise of interest"
                .format(len(missing_rel_data)))
      queue.reset()

  def init_active_queue(self,r,queue,dest,rib_tags,log):
    """
    All the base ASes install their sure paths
    """
    rib_in = PathSet()
    base_ases = set()
    sure_path_key = "sure_path_to:{0}".format(dest)

    ctr = 0
    for rib_tag in rib_tags:
      log.info("Reading RIB Tag '{0}'".format(rib_tag))
      rib_ases = redis_structures.Collection(r,"{0}_ases".format(rib_tag))

      for AS in rib_ases:
        sure_path = rib_ases.get_attr(AS,sure_path_key)
        if sure_path:
          queue.add(AS)
          path = Path(ast.literal_eval(sure_path))
          rib_in.add(AS,path)
          base_ases.add(AS)

          #log.info("Added {0} to AS queue".format(AS))
          ctr += 1

    log.info("{0}/{1} ASes have sure paths to {2}"
             .format(ctr,len(rib_ases),dest))

    return (base_ases,rib_in)

class Path(list):

  VF_LUA = """
  local relation
  local direction = nil

  for i = 1,table.getn(KEYS) do
    relation = redis.call("HGET",KEYS[i],ARGV[i])

    -- If it does'nt exist
    if relation == nil then
      return {false, KEYS[i], ARGV[i], 1}
    end

    if direction == nil then
      if relation == "peer" or relation == "customer" then
        direction = "down"
      elseif relation == "provider" then
        direction = "up"
      end
    elseif direction == "down" then
      -- only customers after down
      if relation ~= "customer" then
        return {false, KEYS[i], ARGV[i], 0}
      end
    elseif direction == "up" then
      if relation == "peer" or relation == "customer" then
        direction = "down"
      end
    end
  end

  return {true,false, false, 0}
  """

  def clone(self):
    cpy = Path(self)
    cpy.sure_count = self.sure_count
    cpy.frequency = self.frequency
    cpy._valley_free = self._valley_free

    # This is wrong, but there's no reason
    # we should ever copy a list that has a loop
    cpy._have_loop = self._have_loop
    cpy._loop_detect = dict([(x,1) for x in self])
    return cpy

  def __init__(self,iterable=[]):
    self.extend(iterable)
    self.sure_count = len(iterable)
    self.frequency = 1

    # Convenience stuff
    self._loop_detect = dict([(x,1) for x in iterable])
    self._valley_free = True
    self._have_loop = False

  def incr_freq(self):
    self.frequency += 1

  def __hash__(self):
    return tuple(self).__hash__()

  @property
  def ulen(self):
    return len(self) - self.sure_count

  @property
  def valid(self):
    if self._have_loop:
      return False
    if not self._valley_free:
      return False
    return True

  def check_valley_free(self,relationship_dict):

    valid = True
    missing_data = False
    direction = None

    for AS1,AS2 in pairwise(self[:self.ulen:]):
      try:
        relation = relationship_dict[AS1][AS2]['rel']
      except KeyError:
        valid = False
        missing_data = (AS1,AS2)
        break

      if direction == None:
        if relation == "peer" or relation == "customer":
          direction = "down"
        elif relation == "provider":
          direction = "up"
      elif direction == "down":
        # only customers after down
        if relation != "customer":
          valid = False
          break;
      elif direction == "up":
        if relation == "peer" or relation == "customer":
          direction = "down"

    return (valid,missing_data)


  def check_valley_free_online(self,redis_info):
    r = redis_info.instantiate()

    vf_args = [ (as_graph.AS_REL(AS1),AS2) for AS1,AS2 in pairwise(self) ]
    pipe = r.pipeline(transaction=False)
    for key,arg in vf_args:
      pipe.hget(key,arg)
    result = pipe.execute()

    valid = True
    missing_data = False
    direction = None

    for i,relation in enumerate(result):
      if relation == None:
        valid = False
        missing_data = vf_args[i]
        break

      if direction == None:
        if relation == "peer" or relation == "customer":
          direction = "down"
        elif relation == "provider":
          direction = "up"
      elif direction == "down":
        # only customers after down
        if relation != "customer":
          valid = False
          break;
      elif direction == "up":
        if relation == "peer" or relation == "customer":
          direction = "down"

    return (valid,missing_data)

    #valid,link1,link2,data_missing = CHECK_VF_SCRIPT(keys=keys,args=args)
    #if not valid:
      #self._valley_free = False

    #return (True if valid else False,link1,link2,data_missing)

  def prepend(self,element,sure=False):
    self.insert(0,element)
    if sure:
      self.sure_count += 1

    if element in self._loop_detect:
      self._loop_detect[element] += 1
      self._have_loop = True
    else:
      self._loop_detect[element] = 1

  def __eq__(self,other):
    return True if tuple(self) == tuple(other) else False

  def __lt__(self,other):
    if len(self) < len(other):
      return True
    if self.ulen < other.ulen:
      return True
    if self.frequency > other.frequency:
      return True
    if float(self[0]) < float(other[0]):
      return True


class PathSet(dict):

  def __init__(self):
    pass

  def add(self,origin,path):
    if origin not in self:
      self[origin] = (dict(),list())

    if path not in self[origin][0]:
      self[origin][1].append(path)
      self[origin][0][path] = path

      self[origin][1].sort()
      return True
    else:
      self[origin][0][path].frequency += 1
    return False

  def pop(self,origin):
    elem = self[origin][1].pop(0)
    self[origin][0].remove(elem)
    return elem

  def peek(self,origin,copy=False):
    if origin not in self:
      return None
    if copy:
      return self[origin][1][0].clone()
    return self[origin][1][0]

def pairwise(iterable):
  "s -> (s0,s1), (s1,s2), (s2, s3), ..."
  a, b = itertools.tee(iterable)
  next(b, None)
  return itertools.izip(a, b)
