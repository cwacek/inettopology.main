class SilentExit(RuntimeError):
  pass

import inettopology.asmap.cmdline

def run():
  """ Be the load point for all commands below this """
  try:
    inettopology.asmap.cmdline.run()
  except SilentExit:
    pass
