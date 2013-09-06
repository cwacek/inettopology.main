import logging
logger = logging.getLogger(__name__)


def add_cmdline_args(subp, parents):
  """Add commandline arguments for this module
  to the subparser :subp:.

  Include :parents: as parents of the parser.

  :subp: argparse.SubParser
  :parents: list of argparse.Parsers
  """

  infer_parser = subp.add_parser("infer",
                                 help="Infer AS level paths",
                                 parents=parents)
  infer_parser.add_argument("--log", help="where to log activity to")
  infer_parser.add_argument("--tags",
                            help="The RIB tags to include above the base",
                            nargs='+', required=True)
  infer_parser.add_argument("--inferrer_count", "-c",
                            help="The number of inferrers per tag",
                            default=1, type=int)
  infer_parser.add_argument("--inferrer_bin",
                            help="The binary to use for inference.",
                            default="./as_infer")
  infer_parser.add_argument("--include-ixps",
                            help="Include notes about IXPs from this datafile",
                            metavar="IXP_DATAFILE")
  infer_parser.add_argument("--translate-ips",
                            help="Include the capability to translate IPs "
                                 "using a MaxMind GeoIP database",
                            metavar="GEOIP_DB")
  existing_elems = infer_parser.add_mutually_exclusive_group()
  existing_elems.add_argument("--force",
                              help="Leave existing elements in the queue",
                              action="store_true")
  existing_elems.add_argument("--reset",
                              help="Clear the queue before starting",
                              action="store_true")
  infer_parser.set_defaults(func=_gao_inference_helper)


def _gao_inference_helper(args):
  """ A helper to allow not importing gao_inference unless
  it's being run. gao_inference uses gevent, and that means
  no PyPy"""
  try:
    import gevent
  except ImportError:
    logger.error("'gevent' not found. Try 'pip install gevent'. "
                 "Note that 'gevent' doesn't work with PyPy")

  import inettopology.asmap.infer.server as infer_server
  infer_server.start_inference_service(args)
