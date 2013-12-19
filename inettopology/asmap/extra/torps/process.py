import os.path
import time
import traceback
import itertools
import json
import sys
import random
import logging
import inettopology.asmap.extra.torps.ixps as ixps

log = logging.getLogger(__name__)

PROC_STARTED = 0
PROC_FINISHED = 0

""" Example Path Simulator output
# Sample  Timestamp       Guard IP        Middle IP       Exit IP Destination IP
# 0       1343865600      81.21.246.66    173.213.78.125  109.174.60.6    0
# 1       1343865600      62.112.195.56   94.23.58.223    85.25.154.219   0
# 2       1343865600      199.48.147.36   213.163.64.43   88.198.52.214   0
# 3       1343865600      38.229.70.34    85.24.215.13    188.138.121.118 0
# 4       1343865600      198.100.145.95  77.247.181.165  166.70.154.130  0
# 5       1343865600      85.25.145.98    145.53.65.130   37.9.170.229    0
# 6       1343865600      131.251.148.37  38.229.79.2     83.170.92.9     0
# 7       1343865600      38.229.70.61    85.24.215.17    31.172.30.3     0
# 8       1343865600      95.109.125.72   66.55.144.151   198.100.156.232 0
# 9       1343865600      188.138.121.118 50.115.125.53   199.48.147.35   0
# 0       1343865600      176.31.118.165  50.115.125.53   85.17.177.73    74.125.131.105
# 1       1343865600      62.112.195.56   81.218.219.122  82.208.89.58    74.125.131.105
# 2       1343865600      199.48.147.36   131.130.199.36  108.174.195.211 74.125.131.105
"""

ixp_data = None


def mk_callback(ptype, endpoints, timestamp, sample):
  def callback(data):
    global PROC_FINISHED
    global ixp_data

    if data['type'] == "error":
      print("@ERROR|{0}:{1}|{2}".format(timestamp, sample, data['msg']))
      return

    path_ixps, path_mixps = ixp_data.identify_ixps(data['path'])
    ixpline = " ".join(path_ixps) if path_ixps is not None else "-"
    metaixpline = " ".join(path_mixps) if path_mixps is not None else "-"

    print("@PATH|{0}::{1}|{2}|{3}|{4}".format(
          endpoints[0],
          endpoints[1],
          data['path'],
          ixpline,
          metaixpline))

    PROC_FINISHED += 1

  return callback


def lookup_missing(args):
  import inettopology.asmap.extra.torps.aspath as aspath
  global ixp_data

  try:
    ixp_data = ixps.IxpDataHandler(args.ixps, args.meta_ixps)
  except Exception as e:
    log.error("Failed to load IXP data [{0}]".format(e))
    sys.exit(1)

  # Instantiate the query engine
  log.info("Starting querier")
  searcher = aspath.ASQuerier(log=log, max_outstanding=20)

  try:
    for fname in args.datafile:
      with open(fname) as fin:
        for i, line in enumerate(fin):
          try:
            e1, e2 = line.strip().split("::")
          except Exception as e:
            log.error("Error on line {0}: {1}".format(i, e))
            continue
          if e1.find(".") != -1:
            # This is an ip-ip path
            searcher.query_by_ip(args.tag, e1, e2,
                                 mk_callback("Exit-Destination",
                                             (e1, e2),
                                             "N/A", "N/A"))
          else:
            searcher.query_mixed(args.tag, (e1, 'AS'), (e2, 'IP'),
                                 mk_callback("Client-Guard",
                                             (e1, e2),
                                             "N/A", "N/A"))

  except KeyboardInterrupt:
    log.warn("Shutting down")
    searcher.shutdown()
    pass
  searcher.shutdown()


def preprocess(args):
  import inettopology.asmap.extra.torps.aspath as aspath
  global PROC_STARTED
  global PROC_FINISHED
  global ixp_data

  try:
    ixp_data = ixps.IxpDataHandler(args.ixps, args.meta_ixps)
  except Exception as e:
    log.error("Failed to load IXP data [{0}]".format(e))
    sys.exit(1)

  # Load the client ASes that we're going to use
  if args.client_as_file:
    possible_client_ases = list()
    sample_as_map = dict()
    try:
      fin = open(args.client_as_file)
    except IOError as e:
      log.error("Failed to open client AS file [{0}]".format(e))
      sys.exit(1)

    for line in fin:
      possible_client_ases.append(line.strip())

    fin.close()

  # Instantiate the query engine
  log.info("Starting querier")
  searcher = aspath.ASQuerier(log=log, max_outstanding=20)

  # Don't repeat lookups
  completed_lookups = dict()
  skipped = 0

  if args.load_paths:
    with open(args.load_paths) as fin:
      for line in fin:
        print(line.strip())
        fields = line.strip().split("|")
        if fields[0] == "@PATH":
          src, dest = fields[1].split("::")
          completed_lookups[(src, dest)] = 1

    log.info("Loaded {0} existing paths".format(len(completed_lookups)))
  #track streams
  unique_streams = dict()
  fctr = 0

  try:
    # Do this for every file in sequence so our cache sticsk around
    for fname in args.datafile:
      try:
        fin = open(fname)
      except IOError as e:
        log.error("Failed to open file [{0}]".format(e))
        sys.exit(1)

      read = 1
      fctr += 1
      skipped = 0

      next(fin)
      for line in fin:
        sample, timestamp, guard, middle, exit, destination = line.split()[:6]

        if destination != "0":
        # We really only care if there are things on both ends.
        # Otherwise it's irrelevant
          if args.client_as_file:
            try:
              client_as = sample_as_map[sample]
            except KeyError:
              client_as = random.choice(possible_client_ases)
              sample_as_map[sample] = client_as
              print("@CLIENT_MAPPING|{0}|{1}".format(sample, client_as))
          else:
            client_as = args.client_as

          # Count how many lines we've read
          read += 1

          log.debug("Have {0} outstanding path lookups".format(len(searcher)))

          if (read % 1000 == 0):
            log.info("File {4}/{5} :: Read/PreviouslySeen/UniqueStreams/Paths: {0}/{1}/{2}/{3}"
                     .format(read, skipped, len(unique_streams), len(completed_lookups), fctr, len(args.datafile)))

          if (client_as, guard, exit, destination) not in unique_streams:

            if (client_as, guard) not in completed_lookups:
              searcher.query_mixed(args.tag, (client_as, 'AS'), (guard, 'IP'),
                                   mk_callback("Client-Guard", (client_as, guard), timestamp, sample))
              log.debug("Querying for path {0}".format((client_as, guard)))
              completed_lookups[(client_as, guard)] = 1
            else:
              log.debug("Skipping lookup for path {0} because we've seen it before"
                        .format((client_as, guard)))
              completed_lookups[(client_as, guard)] += 1

            if (exit, destination) not in completed_lookups:
              searcher.query_by_ip(args.tag, exit, destination,
                                   mk_callback("Exit-Destination", (exit, destination), timestamp, sample))
              log.debug("Querying for path {0}".format((exit, destination)))
              completed_lookups[(exit, destination)] = 1
            else:
              completed_lookups[(exit, destination)] += 1
              log.debug("Skipping lookup for path {0} because we've seen it before"
                        .format((exit, destination)))

              unique_streams[(client_as, guard, exit, destination)] = {'ctr': 1, 'first_observation':timestamp}

          else:
            unique_streams[(client_as, guard, exit, destination)]['ctr'] += 1
            completed_lookups[(client_as, guard)] += 1
            completed_lookups[(exit, destination)] += 1
            skipped += 1
            log.debug("Skipping {0} because we've seen this stream before")

        PROC_STARTED += 2

      fin.close()
  except KeyboardInterrupt:
    log.warn("Shutting down")
    searcher.shutdown()
    pass
  finally:
    log.info("Printing streams")
    for stream in unique_streams:
      print("@STREAM_CTR|{0}::{1}|{2}::{3}|{count}|{timestamp}"
            .format(*stream,
                    count=unique_streams[stream]['ctr'],
                    timestamp=unique_streams[stream]['first_observation']))

    print("@TOTAL_STREAMS|{0}".format(PROC_STARTED / 2))

    for pairing in completed_lookups:
      print("@PAIR_COUNTER|{0}|{1}"
            .format(pairing, completed_lookups[pairing]))


class Path(object):
  def __init__(self, origin, dest, path, ixps):
    self.origin = origin
    self.dest = dest
    self.path = frozenset(path.split())
    if ixps == "-":
      self.ixps = frozenset([])
    else:
      ixpids = [ixp.split(":")[0] for ixp in ixps.split()]
      self.ixps = frozenset(ixpids)


class NewPath(object):
  def __init__(self, origin, dest, path, ixps, metaixps):
    self.origin = origin
    self.dest = dest
    self.path = frozenset(path.split())
    if ixps == "-":
      self.ixps = frozenset([])
    else:
      ixpids = [ixp for ixp in ixps.split()]
      self.ixps = frozenset(ixpids)
    if metaixps == "-":
      self.metaixps = frozenset([])
    else:
      ixpids = [ixp for ixp in metaixps.split()]
      self.metaixps = frozenset(ixpids)

result = None
PATH_WAITING = dict()


def analyze(args):
  global result
  global PATH_WAITING
  paths = dict()

  with open(args.paths) as fin:
    for line in fin:
      fields = line.strip().split("|")
      if fields[0] != '@PATH':
        log.debug("Skipping non-path line: {0}".format(line))
      else:
        origin, dest = fields[1].split("::")
        paths[fields[1]] = NewPath(origin, dest, fields[2], fields[3], fields[4])

  log.info("Read {0} paths ".format(len(paths)))

  with open(args.badguys) as fin:
    badguys = json.load(fin)

  result = dict()
  result['sample_globals'] = [x for x in itertools.repeat({"stream_count": 0, "fail_count": 0}, args.samples)]
  for AS in badguys:
    result[AS] = dict()

    as_adversaries = dict()
    if "AS" in badguys[AS]:
      this_AS_badases = badguys[AS]["AS"]
      for i in xrange(len(this_AS_badases)):
        as_adversaries[frozenset(this_AS_badases[:i+1])] =list()
        for sample in xrange(args.samples):
          as_adversaries[frozenset(this_AS_badases[:i+1])].append({
            'comp_time': None,
            'guard_time': None,
            'exit_time': None,
            "comp_ctr": 0,
            'guard_ctr': 0,
            'exit_ctr': 0,
            'good_ctr': 0
            })
    result[AS]['as_result'] = as_adversaries

    ixp_adversaries = dict()
    if "IXP" in badguys[AS]:
      for i in xrange(len(badguys[AS]["IXP"])):
        ixp_adversaries[frozenset(badguys[AS]["IXP"][:i+1])] = list()
        for sample in xrange(args.samples):
          ixp_adversaries[frozenset(badguys[AS]["IXP"][:i+1])].append({
            'comp_time': None,
            'guard_time': None,
            'exit_time': None,
            "comp_ctr": 0,
            'guard_ctr': 0,
            'exit_ctr': 0,
            'good_ctr': 0
            })

    result[AS]['ixp_result'] = ixp_adversaries

    mixp_adversaries = dict()
    if "MetaIXP" in badguys[AS]:
      for i in xrange(len(badguys[AS]["MetaIXP"])):
        mixp_adversaries[frozenset(badguys[AS]["MetaIXP"][:i+1])] = list()
        for sample in xrange(args.samples):
          mixp_adversaries[frozenset(badguys[AS]["MetaIXP"][:i+1])].append({
            'comp_time': None,
            'guard_time': None,
            'exit_time': None,
            "comp_ctr": 0,
            'guard_ctr': 0,
            'exit_ctr': 0,
            'good_ctr': 0
            })
    result[AS]['metaixp_result'] = mixp_adversaries

    #outfiles[AS] = open("{0}/{1}.adversary.{2}.results".format(args.output_dir,
                                                               #os.path.basename(args.datafile),
                                                               #AS), 'w')

  i = 0
  missing_paths = set()
  data = open(args.datafile, "r", -1)
  timer = time.time()
  try:
    next(data)
    for line in data:
      i += 1
      if i % 10000 == 0:
        newtime = time.time()
        log.info("Read %d lines (%0.2f second iteration)\n"%(i, newtime-timer))
        timer = newtime
      sample, timestamp, guard, middle, exit, destination = line.split()[:6]

      #if first_timestamp is None:
        #first_timestamp = float(timestamp)

      timestamp = int(float(timestamp))
      sample = int(sample)

      result['sample_globals'][sample]['stream_count'] += 1

      try:
        exit_path = paths["%s::%s"%(exit, destination)]
      except KeyError:
        if "%s::%s"%(exit, destination) not in missing_paths:
          sys.stderr.write("MISSING_PATH|{0}::{1}|{2}|{3}\n".format(exit, destination, sample, timestamp))
          missing_paths.add("%s::%s"%(exit, destination))
        exit_path = None

      for client_AS, results in result.iteritems():
        if client_AS == 'sample_globals':
          continue # we don't actually want that
        try:
          guard_path = paths["%s::%s"%(client_AS, guard)]
        except KeyError:
          if "%s::%s"%(client_AS, guard) not in missing_paths:
            sys.stderr.write("MISSING_PATH|{0}::{1}|{2}|{3}\n".format(client_AS, guard, sample, timestamp))
            missing_paths.add("%s::%s"%(client_AS, guard))
          guard_path = None

        if guard_path is None or exit_path is None:
          result['sample_globals'][sample]['fail_count'] += 1
          continue

        for as_adversary in results['as_result']:
          check_safety(results, 'as_result', as_adversary, guard_path.path, exit_path.path, timestamp, sample)

        for ixp_adversary in results['ixp_result']:
          check_safety(results, 'ixp_result', ixp_adversary, guard_path.ixps, exit_path.ixps, timestamp, sample)

        for meta_ixp_adversary in results['metaixp_result']:
          check_safety(results, 'metaixp_result', meta_ixp_adversary, guard_path.metaixps, exit_path.metaixps, timestamp, sample)

  except KeyboardInterrupt:
    log.warn("Writing incomplete results\n")
    print_results(result, args)

  except Exception as e:
    traceback.print_exc()
    raise

  else:
    print_results(result, args)


def ad_hoc_callback(waiting_jobs, paths, results, pathtype, meta_ixps, pathid):
  def callback(data):
    global result

    if data['type'] == "error":
      print("@ERROR|{1}|{0}".format(data['msg'], pathid))
      for results, wait_id in PATH_WAITING[pathid]:
        job = waiting_jobs[wait_id]
        timestamp = wait_id[5]
        sample = wait_id[4]

        result['sample_globals'][sample]['fail_count'] += 1
        del waiting_jobs[wait_id]
      return

    if data['path'] is None:
      print("@ERROR|{1}|{0}".format("Path not computed", pathid))
      for results, wait_id in PATH_WAITING[pathid]:
        job = waiting_jobs[wait_id]
        timestamp = wait_id[5]
        sample = wait_id[4]
        result['sample_globals'][sample]['fail_count'] += 1
        del waiting_jobs[wait_id]
      return

    if len(data['ixps']) == 0:
      ixpline =  "-"
    else:
      ixpline = " ".join(["{0}:({1}, {2}):{3}".format(ixp, ixpdata['as1'], ixpdata['as2'], ixpdata['confidence'].split()[0])
                            for ixp, ixpdata in data['ixps'].iteritems()])

    log.info("Missing path found for {0}::{1}. Will notify {2} waiting on it".format(
             pathid[0], pathid[1], len(PATH_WAITING[pathid])))
    for results, wait_id in PATH_WAITING[pathid]:
      job = waiting_jobs[wait_id]
      timestamp = wait_id[5]
      sample = wait_id[4]

      if pathtype == "guard" :
        if job[0] is not None:
          log.info("Conflict")
          continue
        else:
          job[0] = Path(wait_id[0], wait_id[1], data['path'], ixpline)
          paths["%s::%s" % (wait_id[0], wait_id[1])] = job[0]

      elif pathtype == "exit" :
        if job[1] is not None:
          log.info("Conflict")
          continue
        else:
          job[1] = Path(wait_id[2], wait_id[3], data['path'], ixpline)
          paths["%s::%s" % (wait_id[2], wait_id[3])] = job[1]

      if not all(job):
        continue

      guard_path = job[0]
      exit_path = job[1]

      for as_adversary in results['as_result']:
        check_safety(results, 'as_result', as_adversary, guard_path.path, exit_path.path, timestamp, sample)

      for ixp_adversary in results['ixp_result']:
        check_safety(results, 'ixp_result', ixp_adversary, guard_path.ixps, exit_path.ixps, timestamp, sample)

      for meta_ixp_adversary in results['metaixp_result']:
        guard_meta_ixps = set(map(lambda x: meta_ixps.get(x, None), guard_path.ixps))
        exit_meta_ixps = set(map(lambda x: meta_ixps.get(x, None), guard_path.ixps))
        check_safety(results, 'metaixp_result', meta_ixp_adversary, guard_meta_ixps, exit_meta_ixps, timestamp, sample)

      del waiting_jobs[wait_id]
    del PATH_WAITING[pathid]

  return callback

def print_results(result, args):
  for AS in result:
    if AS == 'sample_globals':
      with open("{0}/client.sample.{1}.globals".format(
          args.output_dir,
          args.filetag if args.filetag else os.path.basename(args.datafile)), 'w') as fout:
        fout.write("# sample stream_count fail_count\n")
        for i, sample in enumerate(result['sample_globals']):
          fout.write("{0} {stream_count} {fail_count}\n".format(i, **sample))

    else:
      for adversary, samples in result[AS]['as_result'].iteritems():
        outfile = open("{0}/client.{1}.as_adversary.{2}.{3}.results".format(
                        args.output_dir,
                        AS,
                        "top{0}".format(len(adversary)),
                        args.filetag if args.filetag else os.path.basename(args.datafile)
                       ), 'w')

        outfile.write("# sample comp_time guard_time exit_time comp_ctr guard_ctr exit_ctr good_ctr\n")
        for sample_num, sample in enumerate(samples):
          outfile.write("{0} {1} {2} {3} {4} {5} {6} {7}\n".format(
                          sample_num + args.sample_start,
                          sample['comp_time'],
                          sample['guard_time'],
                          sample['exit_time'],
                          sample['comp_ctr'],
                          sample['guard_ctr'],
                          sample['exit_ctr'],
                          sample['good_ctr']
                          ))
        outfile.close()

      for adversary, samples in result[AS]['ixp_result'].iteritems():
        outfile = open("{0}/client.{1}.ixp_adversary.{2}.{3}.results".format(
                        args.output_dir,
                        AS,
                        "top{0}".format(len(adversary)),
                        args.filetag if args.filetag else os.path.basename(args.datafile)
                       ), 'w')

        outfile.write("# sample comp_time guard_time exit_time comp_ctr guard_ctr exit_ctr good_ctr\n")
        for sample_num, sample in enumerate(samples):
          outfile.write("{0} {1} {2} {3} {4} {5} {6} {7}\n".format(
                          sample_num + args.sample_start,
                          sample['comp_time'],
                          sample['guard_time'],
                          sample['exit_time'],
                          sample['comp_ctr'],
                          sample['guard_ctr'],
                          sample['exit_ctr'],
                          sample['good_ctr']
                          ))
        outfile.close()

      for adversary, samples in result[AS]['metaixp_result'].iteritems():
        outfile = open("{0}/client.{1}.metaixp_adversary.{2}.{3}.results".format(
                        args.output_dir,
                        AS,
                        "top{0}".format(len(adversary)),
                        args.filetag if args.filetag else os.path.basename(args.datafile) 
                       ), 'w')

        outfile.write("# sample comp_time guard_time exit_time comp_ctr guard_ctr exit_ctr good_ctr\n")
        for sample_num, sample in enumerate(samples):
          outfile.write("{0} {1} {2} {3} {4} {5} {6} {7}\n".format(
                          sample_num + args.sample_start,
                          sample['comp_time'],
                          sample['guard_time'],
                          sample['exit_time'],
                          sample['comp_ctr'],
                          sample['guard_ctr'],
                          sample['exit_ctr'],
                          sample['good_ctr']
                          ))
        outfile.close()

def check_safety(results, result_type, adversary, guard_path, exit_path, timestamp, sample):
  safe = True
  have_guard = False
  sample_dict = results[result_type][adversary][sample]
  if guard_path & adversary:
    curr_timestamp = sample_dict['guard_time'] 
    if curr_timestamp is None or timestamp < curr_timestamp:
      sample_dict['guard_time'] = timestamp
    sample_dict['guard_ctr'] +=1
    safe = False
    have_guard = True

  if exit_path & adversary:
    curr_timestamp = sample_dict['exit_time'] 
    if curr_timestamp is None or timestamp < curr_timestamp:
      sample_dict['exit_time'] = timestamp
    sample_dict['exit_ctr'] +=1
    safe = False

    if have_guard:
      curr_timestamp = sample_dict['comp_time'] 
      if curr_timestamp is None or timestamp < curr_timestamp:
        sample_dict['comp_time'] = timestamp
      sample_dict['comp_ctr'] +=1

  if safe:
    sample_dict['good_ctr'] +=1


