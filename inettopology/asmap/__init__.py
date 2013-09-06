
__all__ = ['DBKEYS']
_OBJ = lambda **kwargs: type('obj', (object,), kwargs)()

DBKEYS = _OBJ(AS_REL_KEYS='as_rel_keys',
              AS_REL=lambda x: "as:{0}:rel".format(x),
              BASE_ASES="base_ases",
              BASE_LINKS="base_as_links",
              TAG_LINKS=lambda x: "{0}_as_links".format(x)
              )
