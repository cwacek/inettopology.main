import itertools
import os
import re
import json
import redis

import logging
log = logging.getLogger(__name__)

from inettopology.asmap import DBKEYS
import inettopology.asmap.util as utils
import inettopology.asmap.util.structures as redis_structures

load_help = """
Load data from datafiles into the database.

A couple different types of data files can be loaded,
and each supports a few different options:
"""

aslinks_help = """
CAIDA AS Links datafiles

  --include-indirect    Include links CAIDA has flagged as
                        'indirect'.

"""

ribfile_help = """
Routeviews RIB files in text format as
          output by 'bgpdump -M'

  --tag                 (required) A tag for the routes parsed
                        from this RIB file. Inference is performed
                        against a specific tag, so this is important.

"""

asrel_help = """
AS Relationships from various sources

AS Relationship Information is established from three datasets
all of which which are optional. First data from Gao inference is
applied, then CAIDA data is overlaid, making corrections as necessary.
Finally, sibling information parsed from WHOIS data is applied, making
corrections again.
"""


def add_cmdline_args(subp, parents):
  """ Add the commandline arguments for this module
  to the subparser :subp:.

  Include :parents: as parents of the parser """

  read_parser = subp.add_parser("load",
                                help=load_help,
                                parents=parents)
  subsub = read_parser.add_subparsers()
  aslinks = subsub.add_parser('aslinks', help=aslinks_help)
  aslinks.add_argument('aslinks', help='AS links datafile', metavar='PATH')
  aslinks.add_argument("--include-indirect",
                       help="Include indirect AS links",
                       action="store_true")
  aslinks.set_defaults(func=_load_data, datatype='aslinks')

  ribfile = subsub.add_parser('ribfile', help=ribfile_help)
  ribfile.add_argument("ribfile",
                       help="RIB datafile",
                       metavar='PATH')
  ribfile.add_argument("-t", "--tag",
                       required=True,
                       help="RIB data tag")
  ribfile.set_defaults(func=_load_data, datatype='ribfile')

  asrel_parser = subsub.add_parser("asrels", help=asrel_help,
                                   parents=parents)
  asrel_parser.add_argument("--gao",
                            help="Output file of GAO relationship inference",
                            required=True)
  asrel_parser.add_argument("--caida", help="CAIDA AS Relationship Datafile")
  asrel_parser.add_argument("--siblings", help="WHOIS sibling match dataset")
  asrel_parser.add_argument("--conflict-log",
                            help="A file to log all conflicts to")

  asrel_parser.set_defaults(func=_load_data, datatype='asrel')


def _load_data(args):
  r = redis.StrictRedis(**args.redis)

  if not r.ping():
    raise Exception("Failed to connect to Redis")

  if args.datatype == 'aslinks':
    read_aslinks(r, args.aslinks, args.include_indirect)
  elif args.datatype == 'ribfile':
    parse_routes(r, args.ribfile, args.tag)
  elif args.datatype == 'asrel':
    load_asrels(r, args.gao, args.caida, args.siblings,
                conflict_log=args.conflict_log)


def read_aslinks(r, filename, include_indirect):
  """ aslinks - CAIDA AS Links datafiles

    --include-indirect    Include links CAIDA has flagged as
                          'indirect'.

  """
  try:
    fin = open(filename)
  except IOError as e:
    raise Exception("Failed to open file: {0}".format(e))

  as_set = redis_structures.Collection(r, DB_KEYS.BASE_ASES)
  as_links = redis_structures.KeyedCollection(r, DB_KEYS.BASE_LINKS)

  for line in fin:
    fields = line.split()
    if fields[0] == "T":
      observed = fields[1:]

    allowable_lines = ("D", "I") if include_indirect else ("D")
    if fields[0] not in allowable_lines:
      continue

    # We need to handle Multi-origin AS' by removing them
    moas_re = re.compile("[, _]")
    if moas_re.search(fields[1]):
      continue
    if moas_re.search(fields[2]):
      continue
    side1 = fields[1]
    side2 = fields[2]

    attrs = list(itertools.repeat({'observed_after': observed[0],
                                   'observed_before': observed[1]},
                                  2))
    as_set.add([side1, side2], attrs)

    as_links.add(side1, [side2], [{'source': 'caida'}])

  fin.close()


def parse_routes(r, ribfile, tag):
  """
  Parse data from :ribfile: and insert it into Redis
  using :r:.

  Tag the data using :tag:

  """
  try:
    fin = open(ribfile)
  except IOError as e:
    raise Exception("Failed to open file: {0}".format(e))

  tag_set = redis_structures.Collection(r, "tags")

  base_as_set = redis_structures.Collection(r, DB_KEYS.BASE_ASES)
  as_set = redis_structures.Collection(r, '{0}_ases'.format(tag))
  as_links = redis_structures.KeyedCollection(r, DB_KEYS.TAG_LINKS(tag))
  only_in_rib = dict()

  linectr = 0
  for line in fin:
    (fmt, date, msg_type, peer_ip,
        peer_as, prefix, raw_path, src) = line.split("|")

    as_path_list = raw_path.split(" ")
    as_path = utils.uniqify(as_path_list,
                            key=lambda x: x.strip("{}"),
                            stopat=lambda x: ", " in x)

    # Don't know what to do with withdrawn routes
    if msg_type == "W":
      continue

    pipe = r.pipeline()
    # Build all the things, then commit them at once for speed
    attrs = list(itertools.repeat({'source': os.path.basename(ribfile)},
                                  len(as_path)))
    as_set.add(as_path, attrs, pipe)

    for i, AS in enumerate(as_path[:-1]):
      as_links.add(AS, [as_path[i + 1]], [{'observed': date}], pipe=pipe)
      as_links.add(as_path[i + 1], [AS], [{'observed': date}], pipe=pipe)

      # In order to have a path, we need at least two nodes
      # between
      for j in xrange(i, len(as_path)):
        if j < i + 1:
          continue
        dest = as_path[j]
        sure_path = as_path[i:j]
        sure_path.append(dest)
        as_set.add_attrs(AS,
                         {"sure_path_to:{0}".format(dest): sure_path},
                         pipe=pipe)

    pipe.execute()
    linectr += 1
    if linectr % 1000 == 0:
      log.info("Processed {0} lines".format(linectr))

  tag_set.add([tag])
  fin.close()


def load_asrels(r, gaofile, caidafile=None, siblingsfile=None, **kwargs):
  """
  Read AS Relationship Information

  AS Relationship Information is established from three datasets
  all of which which are optional. First data from Gao inference is
  applied, then CAIDA data is overlaid, making corrections as necessary.
  Finally, sibling information parsed from WHOIS data is applied, making
  corrections again.
  """
  as_rel_keys = redis_structures.Collection(r, DBKEYS.AS_REL_KEYS)
  conflicts = []

  if as_rel_keys.exists():
    log.warn("There appear to be existing relationships in the database.")
    if not utils.confirm("Do you want to continue anyway?")[0]:
      return 1

# Start with Gao inference results
  log.info("Processing relationships from Gao")
  try:
    gao_data = json.load(open(gaofile))
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

    existing = r.hget(DBKEYS.AS_REL(as1), as2)
    if existing and existing != relation:
      conflicts.append({'as1': as1, 'as2': as2,
                        'old': existing, 'new': relation, 'source': 'gao'})

    r.hset(DBKEYS.AS_REL(as1), as2, relation)

    existing = r.hget(DBKEYS.AS_REL(as2), as1)
    if existing and existing != brelation:
      conflicts.append({'as1': as2, 'as2': as1,
                        'old': existing, 'new': brelation, 'source': 'gao'})

    r.hset(DBKEYS.AS_REL(as2), as1, brelation)

    as_rel_keys.add([DBKEYS.AS_REL(as1), DBKEYS.AS_REL(as2)])
  log.info("Processed {0} relationships".format(len(gao_data)))

  # Overwrite with CAIDA matches
  if caidafile:
    conflicts.extend(_read_caida_asrels(r, caidafile))

  log.info("Processing Sibling data")
  if siblingsfile:
    try:
      sibling_data = json.load(open(siblingsfile))
    except Exception as e:
      log.warn("Failed to load sibling file. [{0}]".format(e))
      return 1

    for sib in sibling_data:

      as1 = sib['as1']['asn'].upper().strip("AS")
      as2 = sib['as2']['asn'].upper().strip("AS")
      relation = 'sibling'
      brelation = 'sibling'

      existing = r.hget(DBKEYS.AS_REL(as1), as2)
      if existing and existing != relation:
        conflicts.append({'as1': as1, 'as2': as2,
                          'old': existing, 'new': relation, 'source': 'WHOIS'})

      r.hset(DBKEYS.AS_REL(as1), as2, relation)

      existing = r.hget(DBKEYS.AS_REL(as2), as1)
      if existing and existing != brelation:
        conflicts.append({'as1': as2, 'as2': as1,
                          'old': existing, 'new': brelation,
                          'source': 'WHOIS'})

      r.hset(DBKEYS.AS_REL(as2), as1, brelation)

      as_rel_keys.add([DBKEYS.AS_REL(as1), DBKEYS.AS_REL(as2)])
    log.info("Processed {0} sibling relationships".format(len(sibling_data)))

  if kwargs['conflict_log']:
    with open(kwargs['conflict_log'], "w") as conflict_out:
      json.dump(conflicts, conflict_out)
  else:
    log.info("Stored AS relationships with {0} conflicts"
             .format(len(conflicts)))


def _read_caida_asrels(r, filename):
  """
  Read in AS relationship data from CAIDA datafile
  """
  try:
    fin = open(filename)
  except IOError as e:
    raise Exception("Failed to open file: {0}".format(e))
  log.info("Processing CAIDA relationships")

  conflicts = []
  as_rel_keys = redis_structures.Collection(r, DBKEYS.AS_REL_KEYS)
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

    existing = r.hget(DBKEYS.AS_REL(as1), as2)
    if existing and existing != frelation:
      conflicts.append({'as1': as1, 'as2': as2,
                        'old': existing, 'new': frelation, 'source': 'caida'})

    r.hset(DBKEYS.AS_REL(as1), as2, frelation)

    existing = r.hget(DBKEYS.AS_REL(as2), as1)
    if existing and existing != brelation:
      conflicts.append({'as1': as2, 'as2': as1,
                        'old': existing, 'new': brelation, 'source': 'caida'})

    r.hset(DBKEYS.AS_REL(as2), as1, brelation)

    as_rel_keys.add([DBKEYS.AS_REL(as1), DBKEYS.AS_REL(as2)])

  log.info("Processed {0} relationships from CAIDA".format(cnt))
  return conflicts
