import os
import sys
import itertools
from datetime import datetime
import time
import argparse
import logging
import operator

EARLIEST_TS = None

log = logging.getLogger(__name__)

def confirm(prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no.

    'resp' should be set to the default value assumed by the caller when
    user simply types ENTER.

    >>> confirm(prompt='Create Directory?', resp=True)
    Create Directory? [y]|n: 
    True
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: 
    False
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: y
    True

    """

    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        ans = raw_input(prompt)
        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            print 'please enter y or n.'
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False

class Stats(object):
  """Docstring for Stats """

  def __init__(self,pairs = False):
    self.stream_ctr = 0
    self.streams_comp_as = 0
    self.streams_comp_ixp = 0
    self.streams_comp_both = 0
    self.pairs = pairs

    self.ctr = {
        "as_pair": {
          "both": dict()
        },
        "ixp_pair": {
          "both": dict()
        },
        "as": {
          "exit": dict(),
          "guard": dict(),
          "both": dict()
          },
        "ixp": {
          "exit": dict(),
          "guard": dict(),
          "both": dict()
        },
        "meta_ixp": {
          "exit": dict(),
          "guard": dict(),
          "both": dict()
          }
        }

  def __str__(self):
    return "{{ 'streams': %d, 'as_comp': %d, 'ixp_comp': %d, 'both_comp': %d, 'stats': %s }}" % (
              self.stream_ctr,
              self.streams_comp_as,
              self.streams_comp_ixp,
              self.streams_comp_both,
              self.ctr)

  def __repr__(self):
    return str(self)

  @staticmethod
  def printdict(stream,d,lim,prepend="",translate=lambda x: x):
    sorted_dict = sorted(d.iteritems(),key=operator.itemgetter(1),reverse=True)
    sorted_dict = map(translate,sorted_dict)
    for i in xrange(min(lim,len(sorted_dict)-1)):
      stream.write("{0} {1} {2}\n".format(prepend,*sorted_dict[i]))

  def print_stats(self,prefix):
    global EARLIEST_TS

    with open("{0}.globals.dat".format(prefix),'w') as fout:
      fout.write("n_streams streams_comp_as streams_comp_ixp streams_comp_both\n")
      fout.write("{0.stream_ctr} {0.streams_comp_as} {0.streams_comp_ixp} {0.streams_comp_both}\n"
                  .format(self))


    for stattype,stats in self.ctr.iteritems():
      with open("{0}.{1}.dat".format(prefix,stattype),'w') as fout:
        columns = [key for key in stats]
        printed = set()
        if EARLIEST_TS is None:
          fout.write("id {0}\n".format(" ".join(map(lambda x: "{0}".format(x), columns))))
        else:
          fout.write("id {0}\n".format(" ".join(map(lambda x: "{0} {0}_obs".format(x), columns))))

        for column in columns:
          for stat_element in stats[column]:
            if stat_element not in printed:
              printed.add(stat_element)

              row = []
              for column in columns:
                elem = stats[column].get(stat_element)
                if elem is not None:
                  if EARLIEST_TS is None:
                    row.append("{0}".format(elem['count']))
                  else:
                    row.append("{0} {1}".format(elem['count'],
                                                time.mktime(elem['first_obs'].timetuple())))
                else:
                  if EARLIEST_TS is None:
                    row.append("0")
                  else:
                    row.append("0 -1")
              fout.write("{0} {1}\n".format(
                          stat_element,
                          " ".join(row)))

  def observe(self,obstype,objid,position,stream):
    count = int(stream.count)

    if obstype not in self.ctr or objid is None:
      raise Exception
    if objid not in self.ctr[obstype][position]:
      self.ctr[obstype][position][objid] = {'count': count, 'first_obs': stream.ts}
    else:
      self.ctr[obstype][position][objid]['count'] += count

  def update(self, stream,meta_ixps):
    """Update stats based on the stream object passed to us

    :stream: a Stream object
    :returns: Nothing

    """
    self.stream_ctr += stream.count
    gpath = stream.guard_path
    epath = stream.exit_path
    as_comp = False
    ixp_comp = False

    for AS in gpath.path & epath.path:
      as_comp = True
      self.streams_comp_as += stream.count
      self.observe("as",AS,"both",stream)

    #for g_as,e_as in itertools.izip_longest(gpath.path,epath.path):
      #if g_as is not None:
        #self.observe("as",g_as,"guard",stream)
      #if e_as is not None:
        #self.observe("as",e_as,"exit",stream)

    for ixp in gpath.ixps & epath.ixps:
      ixp_comp = True
      self.streams_comp_ixp += stream.count
      self.observe("ixp",ixp,"both",stream)

    #for g_ixp,e_ixp in itertools.izip_longest(gpath.ixps,epath.ixps):
      #if g_ixp is not None:
        #self.observe("ixp",g_ixp,"guard",stream)
      #if e_ixp is not None:
        #self.observe("ixp",e_ixp,"exit",stream)

    if as_comp and ixp_comp:
      self.streams_comp_both += stream.count

    for ixp in gpath.metaixps & epath.metaixps:
      self.observe("meta_ixp",ixp,"both",stream)

    #for g_ixp,e_ixp in itertools.izip_longest(gpath.metaixps,epath.metaixps):
      #self.observe("meta_ixp",g_ixp,"guard",stream)
      #self.observe("meta_ixp",e_ixp,"exit",stream)

    if self.pairs:
      for pair in itertools.product(gpath.path,epath.path):
        self.observe("as_pair","%s,%s" % (pair[0],pair[1]),"both",stream)
      for pair in itertools.product(gpath.ixps,epath.ixps):
        self.observe("ixp_pair","%s,%s" %(pair[0],pair[1]),"both",stream)


class Stream(object):
  def __init__(self,guard,exit,count,ts):
    self.guard_link = guard
    self.guard_path = None
    self.exit_link = exit
    self.exit_path = None
    self.ts = ts
    self.count = int(count)

  def update(self,endpoints,path):
    if endpoints == self.guard_link:
      self.guard_path = path
    elif endpoints == self.exit_link:
      self.exit_path = path

    if self.guard_path and self.exit_path:
      return True

    return False

  def __hash__(self):
    return hash("%s::%s" % (self.guard_link, self.exit_link))

  def __eq__(self,other):
    if self.__hash__() == other.__hash__():
      return True
    return False

class Path(object):
  def __init__(self,origin,dest,path,ixps,metaixps,rawdata):
    self.base_data = rawdata
    self.origin = origin
    self.dest = dest
    self.path = frozenset(path.split())
    if ixps == "-":
      self.ixps = frozenset([])
    else:
      ixpids = [ixp for ixp in ixps.split() ]
      self.ixps = frozenset(ixpids)
    if metaixps is None or metaixps == "-":
      self.metaixps = frozenset([])
    else:
      ixpids = [ixp for ixp in metaixps.split() ]
      self.metaixps = frozenset(ixpids)


class WaitList(dict):
  def __init__(self,stats):
    self.stats =stats
    self.waiting_streams = set()

  def add(self,key,obj):
    if key not in self:
      self[key] = list()
    self[key].append(obj)
    self.waiting_streams.add(obj)

  def log_missing(self,stream):
    for key in self:
      stream.write("{0}\n".format(key))

  def __str__(self):
    val = """
    Missing paths: %d
    Streams waiting for guard side: %d (%0.2f)
    Streams waiting for exit side: %d (%0.2f)
    """

    gside =0
    eside=0
    for stream in self.waiting_streams:
      if stream.guard_path is None:
        gside += 1
      elif stream.exit_path is None:
        eside += 1

    return val % (len(self),
                  gside,
                  gside/float(self.stats.stream_ctr),
                  eside,
                  eside/float(self.stats.stream_ctr))

  def process(self,key,answer):
    """
    Process all of the things waiting for this key by
    giving them :answer:.
    """
    if key not in self:
      return
    for stream in self[key]:
      if stream.update(key,answer):
        # if it returns true, we can update stats from it
        self.stats.update(stream)
      self.waiting_streams.remove(stream)

    del self[key]

def process_datafile(fh,stats,args,paths):
  """@todo: Docstring for process_datafile
:stats: The stats object to record information in
  :returns: @todo

  """
  global EARLIEST_TS
  AS_STATS_FILE = "{0.output_prefix}.as_stats.dat".format(args)
  IXP_STATS_FILE = "{0.output_prefix}.ixp_stats.dat".format(args)
  AS_PAIR_FILE = "{0.output_prefix}.as_pair_stats.dat".format(args)
  IXP_PAIR_FILE = "{0.output_prefix}.ixp_pair_stats.dat".format(args)
  waiting = WaitList(stats)
  meta_ixps = None

  for i,line in enumerate(fh):
    if (stats.stream_ctr > 0 and stats.stream_ctr % 1000 == 0) or len(paths) % 1000 == 0:
      log.info("Read %d paths, %d streams, %i lines" % (len(paths),stats.stream_ctr, i))
    fields = line.strip().split("|")
    ltype = fields[0]

    if ltype == "@STREAM_CTR":
      done = False
      guard_link = fields[1]
      exit_link = fields[2]
      count = fields[3]
      if len(fields) > 4:
        timestamp = datetime.fromtimestamp(float(fields[4]))
        if not EARLIEST_TS or timestamp < EARLIEST_TS:
          EARLIEST_TS = timestamp
      else:
        timestamp =None

      stream = Stream(guard_link,exit_link,count,timestamp)
      try:
        stream.update(guard_link,paths[guard_link])
      except KeyError:
        # No path found yet
        waiting.add(guard_link,stream)
        pass

      try:
        done = stream.update(exit_link,paths[exit_link])
      except KeyError:
        # No path found yet
        waiting.add(exit_link,stream)
        pass

      if done:
        stats.update(stream,meta_ixps)
    elif ltype =="@PAIR_COUNTER":
      break

    elif ltype == "@PATH":
      origin,dest = fields[1].split("::")
      paths[fields[1]] = Path(origin,dest,fields[2],fields[3],fields[4] if len(fields) > 4 else None , line.strip())
      waiting.process(fields[1],paths[fields[1]])

  if args.output_prefix:
    stats.print_stats(args.output_prefix)

  else:
    stats.highlight(sys.stdout)
    #stats.printout(sys.stdout)
  if args.log_missing:
    with open(args.log_missing,'w') as fout:
      waiting.log_missing(fout)


def main(args):
  """

  :args: ArgParse args
  :returns: @todo

  """
  paths = dict()

  stats = Stats(args.pairs)
  if args.supplement_paths:
    with open(args.supplement_paths) as fin:
      for line in fin:
        fields = line.strip().split("|")
        if fields[0] != '@PATH':
          log.debug("Skipping non-path line: {0}".format(line))
        else:
          origin,dest = fields[1].split("::")
          paths[fields[1]] = Path(origin,dest,fields[2],fields[3],fields[4],line.strip())

  for datafile in args.datafile:
    f = None
    try:
      f = open(datafile)
    except IOError, e:
      log.error("Failed to open data file '{0}' [{1}]".format(datafile,e))
    else:
      process_datafile(f,stats,args,paths)
    finally:
      if f:
        f.close()

if __name__ == '__main__':

  parser = argparse.ArgumentParser()
  parser.add_argument("datafile",nargs="+",
                      help="Datafiles containing procesed AS paths")
  parser.add_argument("--supplement-paths",metavar="PATHFILE",
                      help="A datafile containing additional paths to consider")
  parser.add_argument("--meta-ixps", metavar="METAIXPFILE",
                      help="Additionally consider meta-ixps from this file")

  parser.add_argument("--output-prefix",
                      help="The prefix to append to output datafiles. If not provided, will write a summary to stdout")

  parser.add_argument("--pairs", help="Investigate pairs of ASes and IXPs",action="store_true")
  parser.add_argument("--log-missing",help="Log missing paths to this file")
  args = parser.parse_args()
  main(args)


