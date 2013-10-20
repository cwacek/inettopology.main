import argparse
import pkg_resources
import logging
logging.basicConfig(level=logging.WARN)


class SilentExit(RuntimeError):
  pass


def run():
  mainparser = argparse.ArgumentParser()
  module_parsers = mainparser.add_subparsers()

  verbose_parser = argparse.ArgumentParser(add_help=False)
  verbose_parser.add_argument("-v", "--verbose", action='count', default=0)

  for ep in pkg_resources.iter_entry_points(group='inettopology.modules'):
    try:
      module = ep.load()
    except:
      pass
    else:
      if '__argparse__' in dir(module) and callable(module.__argparse__):
        mparser = module_parsers.add_parser(ep.name)
        module.__argparse__(mparser.add_subparsers(), [verbose_parser])

  args = mainparser.parse_args()
  if args.verbose > 0:
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG)
  else:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().setLevel(logging.INFO)

  try:
    args.func(args)
  except SilentExit:
    return
