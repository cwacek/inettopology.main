import pkgutil
import importlib
import logging

logger = logging.getLogger(__name__)


def load_cmdline_args(subp, parents=[]):
  """ Load all of the 'extra' modules and
  have them attach themselves to the commandline
  arguments via :subp:
  """
  extra_p = subp.add_parser('extra', help='Additional algorithms and tools')
  extra_sub = extra_p.add_subparsers()

  for importer, modname, ispkg in pkgutil.iter_modules(__path__,
                                                       prefix=__name__ + '.'):

    logger.debug("Found submodule {0} (is a package: {1})"
                 .format(modname, ispkg))

    try:
      mod = importlib.import_module(modname, 'inettopology.asmap.extra')

    except ImportError as e:
      logger .warn("Unable to import '{0}'. [Error: {1}]"
                   .format(modname, e.message))

    else:
      if callable(mod.__argparse__):
        mod.__argparse__(extra_sub, parents)
