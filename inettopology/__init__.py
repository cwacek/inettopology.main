import argparse
import pkg_resources

class SilentExit(RuntimeError):
  pass

def run():
  mainparser = argparse.ArgumentParser()
  module_parsers = mainparser.add_subparsers()

  modules = []
  for ep in pkg_resources.iter_entry_points(group='inettopology.modules'):
    module = ep.load()
    if callable(module.__module_load__):
      mparser = module_parsers.add_parser(ep.name)
      module.__module_load__(mparser.add_subparsers())

  args = mainparser.parse_args()
  if args.verbose > 0:
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG)
  else:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().setLevel(logging.INFO)

  args.func(args)
