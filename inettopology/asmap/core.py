import redis
import logging

from inettopology.asmap import DBKEYS as dbkeys
import inettopology.util.structures as redis_structures

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()
log.setLevel(logging.INFO)


def clean(args):
  """
  Delete all of the Redis keys that represent this graph
  """
  r = redis.StrictRedis(**args.redis)

  if not r.ping():
    raise Exception("Failed to connect to Redis")

  if args.base_links:
    as_set = redis_structures.Collection(r, dbkeys.BASE_ASES)
    as_links = redis_structures.KeyedCollection(r, dbkeys.BASE_LINKS)

    log.info("Cleaning data for {0} ASes".format(len(as_set)))
    for as_key in as_set:
      as_links.delete(as_key)
    as_set.delete()

  if args.as_rel:
    log.info("Cleaning AS relationship data")
    as_rel_keys = redis_structures.Collection(r, dbkeys.AS_REL_KEYS)
    with r.pipeline() as p:
      as_rel_keys.delete(pipe=p)
      p.execute()
    keys = r.keys("as:*:rel")
    for key in keys:
      r.delete(key)

  if args.rib_links:
    for tag in args.rib_links:
      log.info("Cleaning link/path data for RIB {0}".format(tag))

      as_set = redis_structures.Collection(r, '{0}_ases'.format(tag))
      as_links = redis_structures.KeyedCollection(r,
                                                  dbkeys.TAG_LINKS(tag))

      for as_key in as_set:
        as_links.delete(as_key)

      as_set.delete()

      tag_set = redis_structures.Collection(r, "tags")
      tag_set.remove(tag)


def list_misc(args):
  """
  List various pieces of information that might be useful for other commands.
  """

  rinfo = redis_structures.ConnectionInfo(**args.redis)
  r = rinfo.instantiate()
  had_arg = False

  if args.tags:
    tag_set = redis_structures.Collection(r, "tags")
    print("Tags:")
    for tag in tag_set:
      print(" - {0}".format(tag))
    had_arg = True

  if not had_arg:
    print("No print requests provided. "
          "Look at the help and provide an argument")
