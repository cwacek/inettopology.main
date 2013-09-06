
import inettopology.asmap.util.structures as redis_structures
import inettopology.asmap.util as utils
from inettopology.asmap import DBKEYS

import json
import networkx as nx
import ast

import logging
logger = logging.getLogger(__name__)


def _clean_links(r, links):
  """
  Remove all links that have sibling relationship
  """
  link_order = []
  pipe = r.pipeline()
  for link in links:
    pipe.hget(DBKEYS.AS_REL(link[0]), link[1])
    link_order.append(link)
  res = pipe.execute()

  bad_links = filter(lambda x: x[1] == "sibling", zip(link_order, res))
  links -= set(map(lambda x: x[0], bad_links))


def get_known_rel(r, as1, as2):
  """
  Return True if the database already has as1 and as2 a
  a known sibling relationship.
  """
  if r.hexists(DBKEYS.AS_REL(as1), as2):
    return None
  return r.hget(DBKEYS.AS_REL(as1), as2)


def sibling_heuristic(transit, as1, as2, L=5):

# If neither are in here, then they're not sibling
  if (as2, as1) not in transit or (as1, as2) not in transit:
    return False

  if (transit[(as2, as1)] > L and transit[(as1, as2)] > L):
    return True

  if (((as1, as2) in transit and transit[(as1, as2)] <= L)
       and ((as2, as1) in transit and transit[(as2, as1)] <= L)):
    return True

  return False


def p2c_heuristic(transit, as1, as2, L=5):
  if ((as1, as2) not in transit
     or ((as2, as1) in transit and transit[(as2, as1)] > L)):
    return True
  return False


def c2p_heuristic(transit, as1, as2, L=5):
  if (((as1, as2) in transit and transit[(as1, as2)] > L)
     or (as2, as1) not in transit):
    return True
  return False


def mk_graph(args):

  rinfo = redis_structures.ConnectionInfo(**args.redis)
  r = rinfo.instantiate()

  rib_ases = redis_structures.Collection(r, args.tag + "_ases")
  rib_links = redis_structures.KeyedCollection(r, args.tag + "_as_links")

  G = nx.Graph()
  RelGraph = nx.DiGraph()

  all_ases = set()

  logger.info("Loading ASes from database")
  for AS in rib_ases:
    all_ases.add(AS)
  logger.info("{0} ASes loaded".format(len(all_ases)))

  transit = dict()

  logger.info("Building graph from paths ")
  """ Compute the degree for every AS (or at least set us up to do so) """
  for AS in all_ases:
    links = set(map(lambda x: (AS, x), rib_links.members(AS)))
    #_clean_links(r, links)
    G.add_edges_from(links)

  logger.info("Determining transit relationships")
  P = set()
  for AS in all_ases:
    asn, attrs = rib_ases.get(AS)
    for attr in attrs:
      if attr.startswith("sure_path_to"):
        sure_path = ast.literal_eval(attrs[attr])
        P.add(tuple(sure_path))

        degrees = map(lambda x: nx.degree(G, x), sure_path)
        top_prov_idx = degrees.index(max(degrees))

        for i in xrange(top_prov_idx):
          rel = (sure_path[i], sure_path[i + 1])
          try:
            transit[rel] += 1
          except KeyError:
            transit[rel] = 1

        for i in xrange(top_prov_idx, len(sure_path) - 1):
          rel = (sure_path[i + 1], sure_path[i])
          try:
            transit[rel] += 1
          except KeyError:
            transit[rel] = 1

  stats = {'sibling': 0, 'p2c': 0, 'c2p': 0, 'p2p': 0}

  logger.info("Assigning relationships")
  for path in P:
    for as1, as2 in utils.pairwise(path):
      #known_rel = get_known_rel(r, as1, as2)
      if sibling_heuristic(transit, as1, as2, args.L):
        try:
          existing = RelGraph[as1][as2]['rel']
          if existing == 'sibling':
            raise KeyError
          stats[existing] -= 1
          logger.warn("Replacing '{0}' rel of {1} - >{2} with 'sibling'"
                      .format(existing, as1, as2))
        except KeyError:
          pass
        RelGraph.add_edge(as1, as2, {'rel': 'sibling'})
        RelGraph.add_edge(as2, as1, {'rel': 'sibling'})
        stats['sibling'] += 1
      elif p2c_heuristic(transit, as1, as2, args.L):
        try:
          existing = RelGraph[as1][as2]['rel']
          if existing == 'p2c':
            raise KeyError
          stats[existing] -= 1
          logger.warn("Replacing '{0}' rel of {1} - >{2} with 'p2c'"
                    .format(existing, as1, as2))
        except KeyError:
          pass
        RelGraph.add_edge(as1, as2, {'rel': 'p2c'})
        try:
          RelGraph[as2][as1]['rel'] = 'c2p'
        except KeyError:
          RelGraph.add_edge(as2, as1, {'rel': 'c2p'})
        stats['p2c'] += 1
      elif c2p_heuristic(transit, as1, as2, args.L):
        try:
          existing = RelGraph[as1][as2]['rel']
          if existing == 'c2p':
            raise KeyError
          stats[existing] -= 1
          logger.warn("Replacing '{0}' rel of {1} - >{2} with 'c2p'"
                      .format(existing, as1, as2))
        except KeyError:
          pass
        RelGraph.add_edge(as1, as2, {'rel': 'c2p'})
        try:
          RelGraph[as2][as1]['rel'] = 'p2c'
        except KeyError:
          RelGraph.add_edge(as2, as1, {'rel': 'p2c'})
        stats['c2p'] += 1
      else:
        logger.warn("{0} - >{1} Didn't match any of the heuristics???!?!"
                    .format(as1, as2))

  notpeering = set()

  logger.info("Identifying ASes that cannot be peers")

  for path in P:
    degrees = map(lambda x: nx.degree(G, x), path)
    prov_idx = degrees.index(max(degrees))

    for i in xrange(prov_idx - 2):
      notpeering.add((path[i], path[i + 1]))

    for i in xrange(prov_idx + 1, len(path) - 1):
      notpeering.add((path[i], path[i + 1]))

  try:
    if (prov_idx > 0
        and RelGraph[path[prov_idx - 1]][path[prov_idx]]['rel'] != 'sibling'
        and RelGraph[path[prov_idx]][path[prov_idx + 1]]['rel'] != 'sibling'):

      if degrees[prov_idx - 1] > degrees[prov_idx + 1]:
        notpeering.add((path[prov_idx], path[prov_idx + 1]))
      else:
        notpeering.add((path[prov_idx - 1], path[prov_idx]))
  except KeyError:
    import pdb; pdb.set_trace()
    print "Hello"

  logger.info("Assigning p2p relationships")
  for path in P:
    degrees = map(lambda x: float(nx.degree(G, x)), path)
    for i in xrange(len(path) - 1):
      if (path[i], path[i + 1]) not in notpeering:
        if (path[i + 1], path[i]) not in notpeering:
          if (degrees[i] / degrees[i + 1] < args.R
             and degrees[i] / degrees[i + 1] > 1 / args.R):
            try:
              curr = RelGraph[path[i]][path[i + 1]]['rel']
              stats[curr] -= 1
            except KeyError:
              pass
            try:
              RelGraph[path[i]][path[i + 1]]['rel'] = 'p2p'
              RelGraph[path[i + 1]][path[i]]['rel'] = 'p2p'
            except KeyError:
              import pdb; pdb.set_trace()
              print "Blah"
            stats['p2p'] += 1

  logger.info("Determined relationships: "
              "{0} siblings, {1} p2c, {2} c2p, {3} p2p"
              .format(stats['sibling'], stats['p2c'],
                      stats['c2p'], stats['p2p']))

  with open(args.outfile, "w") as fout:
    fout.write("[")
    for i, (as1, as2, attrs) in enumerate(RelGraph.edges_iter(data=True), 1):
      entry = {'as1': as1, 'as2': as2, 'relation': attrs['rel']}
      fout.write(json.dumps(entry))
      if i != RelGraph.number_of_edges():
        fout.write(", \n")
    fout.write("]")


def __argparse__(subp, parents):
  mk_p = subp.add_parser("gao_relation", parents=parents,
                         help="Apply Gao's AS relationship algorithm "
                              "to the data in the database")
  mk_p.add_argument("tag", help="The tag to build the link graph from")
  mk_p.add_argument("-L", help="The value of the transit threshold",
                    type=int, default=1)
  mk_p.add_argument("-R",
                    help="Threshold for difference between AS degrees "
                         "for them to be considered an p2p relationship.",
                    default=60.0)
  mk_p.add_argument("outfile", help="Where to dump output")

  mk_p.set_defaults(func=mk_graph)
