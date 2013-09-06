try:
  from gevent import monkey
  monkey.patch_all()
except ImportError:
  pass

__all__ = ['DBKEYS']
_OBJ = lambda **kwargs: type('obj', (object,), kwargs)()

DBKEYS = _OBJ(
    AS_REL_KEYS='as_rel_keys',
    AS_REL=lambda s, x: "as:{0}:rel".format(x),
    BASE_ASES="base_ases",
    BASE_LINKS="base_as_links",
    TAG_LINKS=lambda sa, x: "{0}_as_links".format(x),
    INFERRED=lambda s, dest, tags: "inferred_to:{0}:tags:{1}".format(
                                   dest, "_".join(tags)),
    INFERRED_KEYS='inferred:keylist'
)
