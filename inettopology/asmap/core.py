import json
import redis
import logging

from inettopology.asmap import dbkeys
from inettopology.util import confirm, redis_structures

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()
log.setLevel(logging.INFO)


def load_asrels(args):
  r = redis.StrictRedis(**args.redis)
  as_rel_keys = redis_structures.Collection(r, dbkeys.AS_REL_KEYS)
  conflicts = []

  if as_rel_keys.exists():
    log.warn("There appear to be existing relationships in the database.")
    if not confirm("Do you want to continue anyway?")[0]:
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

      existing = r.hget(dbkeys.AS_REL(as1), as2)
      if existing and existing != relation:
        conflicts.append({'as1': as1, 'as2': as2,
                          'old': existing, 'new': relation, 'source': 'gao'})

      r.hset(dbkeys.AS_REL(as1), as2, relation)

      existing = r.hget(dbkeys.AS_REL(as2), as1)
      if existing and existing != brelation:
        conflicts.append({'as1': as2, 'as2': as1,
                          'old': existing, 'new': brelation, 'source': 'gao'})

      r.hset(dbkeys.AS_REL(as2), as1, brelation)

      as_rel_keys.add([dbkeys.AS_REL(as1), dbkeys.AS_REL(as2)])
    log.info("Processed {0} relationships".format(len(gao_data)))

  # Overwrite with CAIDA matches
  if args.caida:
    conflicts.extend(read_asrels(r, args.caida))

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
      relation = 'sibling'
      brelation = 'sibling'

      existing = r.hget(dbkeys.AS_REL(as1), as2)
      if existing and existing != relation:
        conflicts.append({'as1': as1, 'as2': as2,
                          'old': existing, 'new': relation, 'source': 'WHOIS'})

      r.hset(dbkeys.AS_REL(as1), as2, relation)

      existing = r.hget(dbkeys.AS_REL(as2), as1)
      if existing and existing != brelation:
        conflicts.append({'as1': as2, 'as2': as1,
                          'old': existing, 'new': brelation,
                          'source': 'WHOIS'})

      r.hset(dbkeys.AS_REL(as2), as1, brelation)

      as_rel_keys.add([dbkeys.AS_REL(as1), dbkeys.AS_REL(as2)])
    log.info("Processed {0} sibling relationships".format(len(sibling_data)))

  if args.conflict_log:
    with open(args.conflict_log, "w") as conflict_out:
      json.dump(conflicts, conflict_out)
  else:
    log.info("Stored AS relationships with {0} conflicts"
             .format(len(conflicts)))


def clean(args):
  """
  Delete all of the Redis keys that represent this graph
  """
  r = redis.StrictRedis(**args.redis)

  if not r.ping():
    raise Exception("Failed to connect to Redis")

  if args.base_links:
    as_set = redis_structures.Collection(r, dbkeys.BASE_ASES)
    as_links = redis_structures.KeyedCollection(r, dbkeys.BASE_LINKS)

    log.info("Cleaning data for {0} ASes".format(len(as_set)))
    for as_key in as_set:
      as_links.delete(as_key)
    as_set.delete()

  if args.as_rel:
    log.info("Cleaning AS relationship data")
    as_rel_keys = redis_structures.Collection(r, dbkeys.AS_REL_KEYS)
    with r.pipeline() as p:
      as_rel_keys.delete(pipe=p)
      p.execute()
    keys = r.keys("as:*:rel")
    for key in keys:
      r.delete(key)

  if args.rib_links:
    for tag in args.rib_links:
      log.info("Cleaning link/path data for RIB {0}".format(tag))

      as_set = redis_structures.Collection(r, '{0}_ases'.format(tag))
      as_links = redis_structures.KeyedCollection(r,
                                                  dbkeys.TAG_LINKS(tag))

      for as_key in as_set:
        as_links.delete(as_key)

      as_set.delete()

      tag_set = redis_structures.Collection(r, "tags")
      tag_set.remove(tag)


def read_asrels(r, filename):
  """
  Read in the AS relationship data
  """
  try:
    fin = open(filename)
  except IOError as e:
    raise Exception("Failed to open file: {0}".format(e))
  log.info("Processing CAIDA relationships")

  conflicts = []
  as_rel_keys = redis_structures.Collection(r, dbkeys.AS_REL_KEYS)
  cnt = 0
  for line in fin:
    if line[0] == "#":
      continue
    as1, as2, rel = line.strip().split("|")
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

    existing = r.hget(dbkeys.AS_REL(as1), as2)
    if existing and existing != frelation:
      conflicts.append({'as1': as1, 'as2': as2,
                        'old': existing, 'new': frelation, 'source': 'caida'})

    r.hset(dbkeys.AS_REL(as1), as2, frelation)

    existing = r.hget(dbkeys.AS_REL(as2), as1)
    if existing and existing != brelation:
      conflicts.append({'as1': as2, 'as2': as1,
                        'old': existing, 'new': brelation, 'source': 'caida'})

    r.hset(dbkeys.AS_REL(as2), as1, brelation)

    as_rel_keys.add([dbkeys.AS_REL(as1), dbkeys.AS_REL(as2)])

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
    tag_set = redis_structures.Collection(r, "tags")
    print("Tags:")
    for tag in tag_set:
      print(" - {0}".format(tag))
    had_arg = True

  if not had_arg:
    print("No print requests provided. "
          "Look at the help and provide an argument")

