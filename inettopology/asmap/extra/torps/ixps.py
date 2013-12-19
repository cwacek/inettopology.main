import logging

log = logging.getLogger(__name__)

from inettopology.util.general import pairwise


class IxpDataHandler(object):

  def __init__(self, ixp_file, metaixp_file):
    self.ixps = dict()
    self.metaixps = dict()

    self.load_ixp_data(ixp_file)
    self.load_metaixp_data(metaixp_file)

  def load_ixp_data(self, filename):
    with open(filename) as fin:

      for line in fin:
        fields = line.split()
        if fields[3] == "bad":
          continue
        peering = tuple(fields[1:3])
        if peering in self.ixps:
          self.ixps[peering].add(fields[0])
        else:
          self.ixps[peering] = set([fields[0]])

  def load_metaixp_data(self, filename):
    with open(filename) as fin:
      for line in fin:
        fields = line.split()
        self.metaixps[fields[0]] = "%s_%s" % (fields[2], fields[1])

  def identify_ixps(self, as_path):
    """Identify the IXP and MetaIXPs that occur along a given AS path

    :as_path: An iterable of AS numbers representing a path
    :returns: A tuple of the form (ixps, metaixps), which are sets
    """
    if as_path is None:
      return ([], [])

    path_ixps = set()

    for pair in pairwise(as_path.split()):
      if pair in self.ixps:
        path_ixps |= self.ixps[pair]

    path_metaixps = set([self.lookup_metaixp(ixp) for ixp in path_ixps])

    return (path_ixps, path_metaixps)

  def lookup_metaixp(self, ixp):
    try:
      return self.metaixps[ixp]
    except KeyError:
      return ixp
