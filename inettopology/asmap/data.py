import itertools
import os
import re
import redis
import redis_structures

import logging
log = logging.getLogger(__name__)

import inettopology.utils as utils


load_help = """
Load data from datafiles into the database.

A couple different types of data files can be loaded,
and each supports a few different options:
"""

aslinks_help = """
aslinks - CAIDA AS Links datafiles

  --include-indirect    Include links CAIDA has flagged as
                        'indirect'.

"""

ribfile_help = """
ribfile - Routeviews RIB files in text format as
          output by 'bgpdump -M'

  --tag                 (required) A tag for the routes parsed
                        from this RIB file. Inference is performed
                        against a specific tag, so this is important.

"""


def add_cmdline_args(subp, parents):
  """ Add the commandline arguments for this module
  to the subparser :subp:.

  Include :parents: as parents of the parser """

  read_parser = subp.add_parser("load",
                                help=load_help + aslinks_help + ribfile_help,
                                parents=parents)
  subsub = read_parser.add_subparsers()
  aslinks = subsub.add_parser('aslinks', help=ribfile_help)
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


def _load_data(args):
  r = redis.StrictRedis(**args.redis)

  if not r.ping():
    raise Exception("Failed to connect to Redis")

  if args.datatype == 'aslinks':
    read_aslinks(r, args.aslinks, args.include_indirect)
  elif args.datatype == 'ribfile':
    parse_routes(r, args.ribfile, args.tag)


def read_aslinks(r, filename, include_indirect):
  try:
    fin = open(filename)
  except IOError as e:
    raise Exception("Failed to open file: {0}".format(e))

  as_set = redis_structures.Collection(r, BASE_ASES)
  as_links = redis_structures.KeyedCollection(r, BASE_LINKS)

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

  base_as_set = redis_structures.Collection(r, BASE_ASES)
  as_set = redis_structures.Collection(r, '{0}_ases'.format(tag))
  as_links = redis_structures.KeyedCollection(r, TAG_LINKS(tag))
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

