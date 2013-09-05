import argparse
import inettopology.asmap.core
import inettopology.asmap.util.structures

__all__ = ['keys', 'cmdline']

_OBJ = lambda **kwargs: type('obj', (object,), kwargs)()

dbkeys = _OBJ(AS_REL_KEYS='as_rel_keys',
              AS_REL=lambda x: "as:{0}:rel".format(x),
              BASE_ASES="base_ases",
              BASE_LINKS="base_as_links",
              TAG_LINKS=lambda x: "{0}_as_links".format(x)
              )


def cmdline():
  parser = argparse.ArgumentParser()
  gen_p = argparse.ArgumentParser(add_help=False)
  gen_p.add_argument("--redis", action=redis_structures.RedisArgAction,
                     default={'host': 'localhost', 'port': 6379, 'db': 0},
                     help="Redis connection info for router server "
                          "(default: 'localhost:6379:0')")

  # Loading data
  subp = parser.add_subparsers(help="Commands")

  inettopology.asmap.data.add_cmdline_args(subp)
  inettopology.asmap.infer.add_cmdline_args(subp)

  asrel_help = """
  Read AS Relationship Information

  AS Relationship Information is established from three datasets
  all of which which are optional. First data from Gao inference is
  applied, then CAIDA data is overlaid, making corrections as necessary.
  Finally, sibling information parsed from WHOIS data is applied, making
  corrections again.
  """
  asrel_parser = subp.add_parser("read_asrel", help=asrel_help,
                                 parents=[gen_p])
  asrel_parser.add_argument("--caida", help="CAIDA AS Relationship Datafile")
  asrel_parser.add_argument("--gao",
                            help="Output file of GAO relationship inference")
  asrel_parser.add_argument("--siblings", help="WHOIS sibling match dataset")
  asrel_parser.add_argument("--conflict-log",
                            help="A file to log all conflicts to")
  asrel_parser.set_defaults(func=load_asrels)

  # Database cleanup
  clean_parser = subp.add_parser("clean", help="Clean the graph data",
                                 parents=[gen_p])
  clean_parser.add_argument("--base_links",
                            help="Clean base links from CAIDA",
                            action="store_true")
  clean_parser.add_argument("--as_rel",
                            help="Clean AS relationship data",
                            action="store_true")
  clean_parser.add_argument("--rib_links",
                            help="Clean links from ROUTEVIEWS RIB files",
                            nargs='+')
  clean_parser.set_defaults(func=clean)

  list_parser = subp.add_parser("list", help="List miscellaneous information",
                                parents=[gen_p])
  list_parser.add_argument("--tags", help="List the RIB tags that exist",
                           action="store_true")
  list_parser.set_defaults(func=list_misc)

  args = parser.parse_args()

  args.func(args)

if __name__ == '__main__':
  cmdline()
