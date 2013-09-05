import itertools
import json
import re
import redis
import redis_structures
import argparse
import logging
import os
import inettopology.util

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()
log.setLevel(logging.INFO)

BASE_ASES = "base_ases"
BASE_LINKS = "base_as_links"
TAG_LINKS = lambda x: "{0}_as_links".format(x)
AS_REL_KEYS = "as_rel_keys"
AS_REL = lambda x: "as:{0}:rel".format(x)


def _gao_inference_helper(args):
  """ A helper to allow not importing gao_inference unless
  it's being run. gao_inference uses gevent, and that means
  no PyPy"""
  import gao_inference
  gao_inference.infer_routes(args)


def load_data(args):
  r = redis.StrictRedis(**args.redis)

  if not r.ping():
    raise Exception("Failed to connect to Redis")

  if args.aslinks:
    read_aslinks(r, args.aslinks, args.include_indirect)


def load_asrels(args):
  r = redis.StrictRedis(**args.redis)
  as_rel_keys = redis_structures.Collection(r, AS_REL_KEYS)
  conflicts = []

  if as_rel_keys.exists():
    log.warn("There appear to be existing relationships in the database.")
    if not inettopology.util.confirm("Do you want to continue anyway?")[0]:
      return 1

# Start with Gao inference results
  if args.gao:
    log.info("Processing relationships from Gao")
    try:
      gao_data = json.load(open(args.gao))
    except Exception as e:
      log.warn("Failed to load gao file. [{0}]".format(e))
      return 1

    for rel in gao_data:
      as1 = rel['as1']
      as2 = rel['as2']
      relation = rel['relation']

      if relation == 'p2c':
        brelation = 'c2p'
      elif relation == 'c2p':
        brelation = 'p2c'
      elif relation == 'sibling':
        brelation = relation
      elif relation == 'p2p':
        brelation = relation

      existing = r.hget(AS_REL(as1), as2)
      if existing and existing != relation:
        conflicts.append({'as1': as1, 'as2': as2,
                          'old': existing, 'new': relation, 'source': 'gao'})

      r.hset(AS_REL(as1), as2, relation)

      existing = r.hget(AS_REL(as2), as1)
      if existing and existing != brelation:
        conflicts.append({'as1': as2, 'as2': as1,
                          'old': existing, 'new': brelation, 'source': 'gao'})

      r.hset(AS_REL(as2), as1, brelation)

      as_rel_keys.add([AS_REL(as1),AS_REL(as2)])
    log.info("Processed {0} relationships".format(len(gao_data)))


  # Overwrite with CAIDA matches
  if args.caida:
    conflicts.extend(read_asrels(r,args.caida))

  log.info("Processing Sibling data")
  if args.siblings:
    try:
      sibling_data = json.load(open(args.siblings))
    except Exception as e:
      log.warn("Failed to load sibling file. [{0}]".format(e))
      return 1

    for sib in sibling_data:

      as1 = sib['as1']['asn'].upper().strip("AS")
      as2 = sib['as2']['asn'].upper().strip("AS")
      relation='sibling'
      brelation='sibling'

      existing = r.hget(AS_REL(as1),as2)
      if existing and existing != relation:
        conflicts.append({'as1':as1, 'as2':as2,
                          'old': existing, 'new': relation, 'source': 'WHOIS'})

      r.hset(AS_REL(as1),as2,relation)

      existing = r.hget(AS_REL(as2),as1)
      if existing and existing != brelation:
        conflicts.append({'as1':as2, 'as2':as1,
                          'old': existing, 'new': brelation, 'source': 'WHOIS'})

      r.hset(AS_REL(as2),as1,brelation)

      as_rel_keys.add([AS_REL(as1),AS_REL(as2)])
    log.info("Processed {0} sibling relationships".format(len(sibling_data)))

  if args.conflict_log:
    with open(args.conflict_log, "w") as conflict_out:
      json.dump(conflicts,conflict_out)
  else:
    log.info("Stored AS relationships with {0} conflicts".format(len(conflicts)))

def read_aslinks(r,filename,include_indirect):
  try:
    fin = open(filename)
  except IOError as e:
    raise Exception("Failed to open file: {0}".format(e))

  as_set = redis_structures.Collection(r,BASE_ASES)
  as_links = redis_structures.KeyedCollection(r,BASE_LINKS)

  for line in fin:
    fields = line.split()
    if fields[0] == "T":
      observed = fields[1:]

    allowable_lines = ("D","I") if include_indirect else ("D")
    if fields[0] not in allowable_lines:
      continue

    # We need to handle Multi-origin AS' by removing them
    moas_re = re.compile("[,_]")
    if moas_re.search(fields[1]):
      continue
    if moas_re.search(fields[2]):
      continue
    side1 = fields[1]
    side2 = fields[2]

    attrs = list(itertools.repeat({'observed_after': observed[0],
                                   'observed_before': observed[1]},
                                  2))
    as_set.add([side1,side2], attrs)

    as_links.add(side1,[side2],[{'source':'caida'}])

  fin.close()

def clean(args):
  """
  Delete all of the Redis keys that represent this graph
  """
  r = redis.StrictRedis(**args.redis)

  if not r.ping():
    raise Exception("Failed to connect to Redis")

  if args.base_links:
    as_set = redis_structures.Collection(r,BASE_ASES)
    as_links = redis_structures.KeyedCollection(r,BASE_LINKS)

    log.info("Cleaning data for {0} ASes".format(len(as_set)))
    for as_key in as_set:
      as_links.delete(as_key)
    as_set.delete()

  if args.as_rel:
    log.info("Cleaning AS relationship data")
    as_rel_keys = redis_structures.Collection(r,AS_REL_KEYS)
    with r.pipeline() as p:
      as_rel_keys.delete(pipe=p)
      p.execute()
    keys = r.keys("as:*:rel")
    for key in keys:
      r.delete(key)

  if args.rib_links:
    for tag in args.rib_links:
      log.info("Cleaning link/path data for RIB {0}".format(tag))

      as_set = redis_structures.Collection(r,'{0}_ases'.format(tag))
      as_links = redis_structures.KeyedCollection(r,TAG_LINKS(tag))

      for as_key in as_set:
        as_links.delete(as_key)

      as_set.delete()

      tag_set = redis_structures.Collection(r,"tags")
      tag_set.remove(tag)

def parse_routes(args):
  """
  Parse data from a RIB file.

  This includes a couple types of data, including AS connections and
  sure paths.
  """
  r = redis.StrictRedis(**args.redis)

  if not r.ping():
    raise Exception("Failed to connect to Redis")

  try:
    fin = open(args.ribfile)
  except IOError as e:
    raise Exception("Failed to open file: {0}".format(e))

  tag_set = redis_structures.Collection(r,"tags")

  base_as_set = redis_structures.Collection(r,BASE_ASES)
  as_set = redis_structures.Collection(r,'{0}_ases'.format(args.tag))
  as_links = redis_structures.KeyedCollection(r,TAG_LINKS(args.tag))
  only_in_rib = dict()

  linectr = 0
  for line in fin:
    (fmt,date,msg_type,peer_ip,peer_as,prefix,raw_path,src) = line.split("|")
    as_path_list = raw_path.split(" ")
    as_path = uniqify(as_path_list,lambda x: x.strip("{}"),stopat=lambda x: "," in x)

    # Don't know what to do with withdrawn routes
    if msg_type == "W":
      continue

    pipe = r.pipeline()
    # Build all the things, then commit them at once for speed
    attrs = list(itertools.repeat({'source': os.path.basename(args.ribfile)},len(as_path)))
    as_set.add(as_path,attrs,pipe)

    for i,AS in enumerate(as_path[:-1]):
      as_links.add(AS,[as_path[i+1]],[{'observed':date}],pipe=pipe)
      as_links.add(as_path[i+1],[ AS ],[{'observed':date}],pipe=pipe)

      # In order to have a path, we need at least two nodes
      # between
      for j in xrange(i,len(as_path)):
        if j < i+1:
          continue
        dest = as_path[j]
        sure_path = as_path[i:j]
        sure_path.append(dest)
        as_set.add_attrs(AS,{"sure_path_to:{0}".format(dest): sure_path},pipe=pipe)

    pipe.execute()
    linectr += 1
    if linectr % 1000 == 0:
      log.info("Processed {0} lines".format(linectr))

  tag_set.add([args.tag])
  fin.close()

def read_asrels(r,filename):
  """
  Read in the AS relationship data
  """
  try:
    fin = open(filename)
  except IOError as e:
    raise Exception("Failed to open file: {0}".format(e))
  log.info("Processing CAIDA relationships")

  conflicts = []
  as_rel_keys = redis_structures.Collection(r,AS_REL_KEYS)
  cnt = 0
  for line in fin:
    if line[0] == "#":
      continue
    as1,as2,rel = line.strip().split("|")
    cnt += 1

    if rel == "0":
      frelation = 'p2p'
      brelation = 'p2p'
    elif rel == '2':
      frelation = 'sibling'
      brelation = 'sibling'
    else:
      frelation = 'p2c'
      brelation = 'c2p'

    existing = r.hget(AS_REL(as1),as2)
    if existing and existing != frelation:
      conflicts.append({'as1':as1, 'as2':as2,
                        'old': existing, 'new': frelation, 'source': 'caida'})

    r.hset(AS_REL(as1),as2,frelation)

    existing = r.hget(AS_REL(as2),as1)
    if existing and existing != brelation:
      conflicts.append({'as1':as2, 'as2':as1,
                        'old': existing, 'new': brelation, 'source': 'caida'})

    r.hset(AS_REL(as2),as1,brelation)

    as_rel_keys.add([AS_REL(as1),AS_REL(as2)])


  log.info("Processed {0} relationships from CAIDA".format(cnt))
  return conflicts

def list_misc(args):
  """
  List various pieces of information that might be useful for other commands.
  """

  rinfo = redis_structures.ConnectionInfo(**args.redis)
  r = rinfo.instantiate()
  had_arg = False

  if args.tags:
    tag_set = redis_structures.Collection(r,"tags")
    print("Tags:")
    for tag in tag_set:
      print(" - {0}".format(tag))
    had_arg = True


  if not had_arg:
    print("No print requests provided. Look at the help and provide an argument")


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  gen_p = argparse.ArgumentParser(add_help=False)
  gen_p.add_argument("--redis",action=redis_structures.RedisArgAction,
                      default={'host':'localhost','port':6379,'db':0},
                      help="Redis connection info for router server "
                           "(default: 'localhost:6379:0')")

  subp = parser.add_subparsers(help="Commands")
  read_parser = subp.add_parser("load",help="Load Data", parents= [gen_p])
  read_parser.add_argument("--aslinks",help="CAIDA AS Links Datafile")
  read_parser.add_argument("--include-indirect",help="Include indirect AS links",
                           action="store_true")
  read_parser.set_defaults(func=load_data)

  asrel_help = """
  Read AS Relationship Information

  AS Relationship Information is established from three datasets
  all of which which are optional. First data from Gao inference is 
  applied, then CAIDA data is overlaid, making corrections as necessary.
  Finally, sibling information parsed from WHOIS data is applied, making
  corrections again.
  """
  asrel_parser = subp.add_parser("read_asrel",help=asrel_help,
                                 parents =[gen_p])
  asrel_parser.add_argument("--caida",help="CAIDA AS Relationship Datafile")
  asrel_parser.add_argument("--gao",help="Output file of GAO relationship inference")
  asrel_parser.add_argument("--siblings",help="WHOIS sibling match dataset")
  asrel_parser.add_argument("--conflict-log",help="A file to log all conflicts to")
  asrel_parser.set_defaults(func=load_asrels)

  clean_parser= subp.add_parser("clean",help="Clean the graph data", parents= [gen_p])
  clean_parser.add_argument("--base_links",
                            help="Clean base links from CAIDA",
                            action="store_true")
  clean_parser.add_argument("--as_rel",
                            help="Clean AS relationship data",
                            action="store_true")
  clean_parser.add_argument("--rib_links",
                            help="Clean AS relationship data ROUTEVIEWS RIB files",
                            nargs='+')
  clean_parser.set_defaults(func=clean)

  route_parser = subp.add_parser("load_ribs",help="Add data from RIBs", parents= [gen_p])
  route_parser.add_argument("ribfile",help="The RIB file in text format as output by bgpdump -M")
  route_parser.add_argument("tag",help="The string to tag these entries with (probably a timestamp)")
  route_parser.set_defaults(func=parse_routes)

  infer_parser = subp.add_parser("infer",help="Infer AS level paths", parents= [gen_p])
  infer_parser.add_argument("--log",help="where to log activity to")
  infer_parser.add_argument("--tags",help="The RIB tags to include above the base",
                            nargs='+',required=True)
  infer_parser.add_argument("--inferrer_count","-c",
                            help="The number of inferrers per tag",
                            default=1,type=int)
  infer_parser.add_argument("--inferrer_bin",
                            help="The binary to use for inference.",
                            default="./as_infer")
  infer_parser.add_argument("--include-ixps",
                            help="Include notes about IXP crossings from this datafile",
                            metavar="IXP_DATAFILE")
  infer_parser.add_argument("--translate-ips",
                            help="Include the capability to translate IP addresses in queries "
                                 "using a MaxMind GeoIP database",
                            metavar="GEOIP_DB")
  existing_elems = infer_parser.add_mutually_exclusive_group()
  existing_elems.add_argument("--force",
                            help="Leave existing elements int the processing queue",
                            action="store_true")
  existing_elems.add_argument("--reset",
                            help="Clear the processing queue before starting inferrers", 
                            action="store_true")
  infer_parser.set_defaults(func=_gao_inference_helper)

  list_parser= subp.add_parser("list",help="List miscellaneous information", parents= [gen_p])
  list_parser.add_argument("--tags",help="List the RIB tags that exist",action="store_true")
  list_parser.set_defaults(func=list_misc)

  args = parser.parse_args()

  args.func(args)
