import argparse
import logging

import inettopology.asmap.util
import inettopology.asmap.core
import inettopology.asmap.data
import inettopology.asmap.infer
import inettopology.asmap.util.structures as structures
import inettopology.asmap.extra


def run():

  parser = argparse.ArgumentParser()
  gen_p = argparse.ArgumentParser(add_help=False)
  gen_p.add_argument("--redis", action=structures.RedisArgAction,
                     default={'host': 'localhost', 'port': 6379, 'db': 0},
                     help="Redis connection info for router server "
                          "(default: 'localhost:6379:0')")

  gen_p.add_argument("-v", "--verbose", action='count')

  # Loading data
  subp = parser.add_subparsers(help="Commands")

  inettopology.asmap.data.add_cmdline_args(subp, [gen_p])
  inettopology.asmap.infer.add_cmdline_args(subp, [gen_p])
  inettopology.asmap.extra.load_cmdline_args(subp, [gen_p])

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
  clean_parser.set_defaults(func=inettopology.asmap.core.clean)

  list_parser = subp.add_parser("list", help="List miscellaneous information",
                                parents=[gen_p])
  list_parser.add_argument("--tags", help="List the RIB tags that exist",
                           action="store_true")
  list_parser.set_defaults(func=inettopology.asmap.core.list_misc)

  args = parser.parse_args()
  if args.verbose > 0:
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.INFO)

  args.func(args)

if __name__ == '__main__':
  run()
