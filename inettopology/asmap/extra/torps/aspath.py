#import socket
import sys
import gevent
import gevent.queue
import gevent.event
import gevent.socket
import json

class ASQueryError(Exception):
    pass

class ConnectionError(ASQueryError):
    pass

class RequestError(ASQueryError):
    pass

class ASQuerier(object):

    _req = {
            'type':'request',
            'tag':None,
            'src':None,
            'dst':None
           }
    _s = None

    def shutdown(self):
      self._shutdown.set()
      self.log.info("Waiting for workers to clear the queue ({0} items) and exit"
                      .format(self.workqueue.qsize()))
      gevent.joinall(self.workers)

    def __init__(self,  log=None, host="localhost", port=9323, max_outstanding = 20):
        """
        Initialize a query interface to make requests to the path
        inference server located at host:port.
        """

        self._addr = (host,port)
        self._s = None
        self._sync = None
        self.log = log
        self._shutdown = gevent.event.Event()
        self._shutdown.clear()
        self.workqueue = gevent.queue.Queue(maxsize = max_outstanding)
        self.workers = []
        for i in xrange(max_outstanding):
          self.workers.append(gevent.Greenlet(ASQuerier.__worker,self._addr,self.log,self.workqueue,self._shutdown))
          #self.workers[i].link_value(callback)
          self.workers[i].start()

        gevent.sleep(0.1)

    def __len__(self):
      return self.workqueue.qsize()

    def max(self):
      return self.workqueue.maxsize

    @staticmethod
    def __worker(addr,log,workqueue,shutdown_event):
    #def __worker(s,tag,src,dst,addr_type):
      log.info("Worker {0} started".format(id(gevent.getcurrent())))

      while True:
        try:
          callback,tag,src,dst,addr_type  = workqueue.get(timeout=10)
        except gevent.queue.Empty:
          if shutdown_event.isSet():
            log.info("Worker {0} shutting down".format(id(gevent.getcurrent())))
            return
          continue

        data= {"type":"error","msg":"Incomplete"}
        log.info("Worker got request for path {0}->{1}".format(src,dst))
        s = gevent.socket.create_connection(addr)

        if addr_type != "defined":
          src = (src,addr_type)
          dst = (dst,addr_type)

        try:
          req = ASQuerier._req
          req.update(tag=tag,src=src,dst=dst)

          s.sendall(json.dumps(req))

          gevent.socket.wait_read(s.fileno(),timeout=180)

          resp = s.recv(2048)
          try:
            data = json.loads(resp)
          except ValueError:
            data = {'type':'error',
                    'msg':"Failed to read response '{0}'".
                                  format(resp)
                   }

          if data['type'] != "error" and 'path' not in data:
            data = {'type':'error',
                    'msg':"Response not understood '{0}'"
                                  .format(resp)
                   }

        except Exception as e:
          sys.stderr.write("Caught exception {0}.\n".format(e))
          sys.stderr.write("Returning data {1}\n".format(data))
        finally:
          s.close()
          callback(data)
          #return data

    @staticmethod
    def watch_ready(q):

      while True:
        if q.ready_event.isSet() and q.outstanding > q.max:
          q.ready_event.clear()
        elif q.outstanding < max/4:
          q.ready_event.set()
        else:
          pass
        gevent.sleep(0.1)

    def query_mixed(self,tag,src,dst,callback):
      """
      Request the query server for the path between
      *src* and *dst*, within the set of paths tagged
      with *tag*.

      *src* and *dst* should be tuples of the form
      (address, type), where type can be either 'AS'
      or 'IP'.
      """
      self.workqueue.put((callback,tag,src,dst,"defined"))
      return
      #self.ready_event.wait()
      self.outstanding +=1
      s = gevent.socket.create_connection(self._addr)
      gr = gevent.spawn(ASQuerier.__worker,s,tag,src,dst,addr_type="defined")
      gr.link_value(lambda x: callback(x.value))
      gr.link(self.mark_completed)
      gevent.sleep(0)

    def mark_completed(self, *args):
      self.outstanding -= 1

    def query_by_ip(self,tag,src,dst,callback):
      """ Request the query server for the
      path between the IP addresses *src* and *dst*, within the set of paths
      tagged with *tag*.

      Returns the path.
      """
      #self.ready_event.wait()
      self.workqueue.put((callback,tag,src,dst,"IP"))
      return
      self.outstanding +=1
      s = gevent.socket.create_connection(self._addr)
      gr = gevent.spawn(ASQuerier.__worker,s,tag,src,dst,addr_type="IP")
      gr.link_value(lambda x: callback(x.value))
      gr.link(self.mark_completed)
      gevent.sleep(0)

    def query_by_as(self,tag,src,dst,callback):
      """
      Request the query server for the path between
      *src* and *dst*, within the set of paths tagged with
      *tag*.

      Returns the path.
      """
      #self.ready_event.wait()
      self.workqueue.put((callback,tag,src,dst,"AS"))
      return
      self.outstanding +=1
      s = gevent.socket.create_connection(self._addr)
      gr = gevent.spawn(ASQuerier.__worker,s,tag,src,dst,addr_type="AS")
      gr.link_value(lambda x: callback(x.value))
      gr.link(self.mark_completed)
      gevent.sleep(0)

