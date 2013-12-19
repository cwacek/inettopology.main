def _preprocess(args):
  import inettopology.asmap.extra.torps.process as processor
  processor.preprocess(args)


def _process_missing(args):
  import inettopology.asmap.extra.torps.process as processor
  processor.lookup_missing(args)


def _analyze(args):
  import inettopology.asmap.extra.torps.analyze_as_paths as analyzer
  analyzer.main(args)


def _postprocess(args):
  import inettopology.asmap.extra.torps.process as processor
  processor.analyze(args)


def __argparse__(subp, parents):

  missing_parser = subp.add_parser("torps.infer_path",
                                   help="Infer a path between two endpoints on an ad-hoc "
                                        "basis. Expects a file with '<endpoint1>::<endpoint2>' "
                                        "on each line.",
                                   parents=parents)

  missing_parser.add_argument("tag",
                              help="The tag to search for the AS path within. "
                                   "Each set of AS paths contains a tag that "
                                   "identifies it. A valid tag needs to be "
                                   "provided to perform searches.")

  missing_parser.add_argument(
      "--ixps",
      help="Identify IXPs from this datafile, which has the format "
           "<ixpid> <as1> <as2> <confidence> <datasource>",
      metavar="IXP_DATAFILE",
      required=True)

  missing_parser.add_argument("--meta-ixps", metavar="METAIXPFILE",
                              help="A datafile containing data about "
                                   "meta-ixps. "
                                   "These are organizations of IXPS",
                              required=True)

  missing_parser.add_argument("datafile", help="Endpoints file", nargs="+")
  missing_parser.set_defaults(func=_process_missing)

  pre_parser = subp.add_parser("torps.preprocess",
                               help="Process paths, identifying the AS path "
                                    "for each one. Print out each unique path "
                                    "and unique stream.",
                               parents=parents)

  pre_parser.add_argument("tag",
                          help="The tag to search for the AS path within. "
                               "Each set of AS paths contains a tag that "
                               "identifies it. A valid tag needs to be "
                               "provided to perform searches.")

  pre_parser.add_argument("--ixps",
                          help="Identify IXPs from this datafile, which has the format "
                               "<ixpid> <as1> <as2> <confidence> <datasource>",
                          metavar="IXP_DATAFILE",
                          required=True)

  pre_parser.add_argument("--meta-ixps", metavar="METAIXPFILE",
                           help="A datafile containing data about meta-ixps. "
                                "These are organizations of IXPS",
                           required=True)

  meg = pre_parser.add_mutually_exclusive_group(required=True)
  meg.add_argument("--client_as_file",
                   help='A file containing one line per AS where clients are '
                        "located. Each client in the simulation will be "
                        "chosen as having originated from one of those ASes "
                        "at random"
                   )

  meg.add_argument("--client_as",
                   help="The client AS for all samples in this trace")

  pre_parser.add_argument("--load_paths",
                          help="Load already processed paths from this file")
  pre_parser.add_argument("datafile", help="Simulation output file", nargs="+")
  pre_parser.set_defaults(func=_preprocess)

  post_parser = subp.add_parser("torps.analyze",
                                help="Count bad things in trace files",
                                parents=parents)

  post_parser.add_argument("datafile", help="The datafile to process")
  post_parser.add_argument("tag",
                           help="The tag to search for the AS path within. "
                                "Each set of AS paths contains a tag that "
                                "identifies it. A valid tag needs to be "
                                "provided to perform searches.")

  post_parser.add_argument("--samples",
                           help="The number of samples in the file",
                           required=True, type=int)

  post_parser.add_argument("--sample-start",
                           help="Pretend the samples start at this number "
                                "(even if they start at 0)",
                           type=int,
                           default=0)

  post_parser.add_argument("--paths", metavar="PATHFILE",
                           help="A file containing preprocessed paths",
                           required=True)

  post_parser.add_argument("--badguys",
                           help="A JSON dataset the ASes, IXPs, and metaIXPS "
                                "for all of the clients we want to use",
                           required=True)

  post_parser.add_argument("--output_dir")
  post_parser.set_defaults(func=_postprocess)

  analyze_parser = subp.add_parser("torps.find_adversaries",
                                   help="Find adversaries who pose "
                                        "threats to clients",
                                   parents=parents)
  analyze_parser.add_argument("datafile", nargs="+",
                              help="Datafiles containing procesed AS paths")
  analyze_parser.add_argument(
      "--paths",
      metavar="PATHFILE",
      help="A datafile preprocesses AS paths. "
           "Any missing can be logged to a file.")

  analyze_parser.add_argument("--meta-ixps", metavar="METAIXPFILE",
                              help="Additionally consider meta-ixps "
                                   "from this file")

  analyze_parser.add_argument("--output-prefix",
                              help="The prefix to append to output datafiles. "
                                   "If not provided, will write a "
                                   "summary to stdout")

  analyze_parser.add_argument("--pairs",
                              help="Investigate pairs of ASes and IXPs",
                              action="store_true")
  analyze_parser.add_argument("--log-missing",
                              help="Log missing paths to this file")

  analyze_parser.set_defaults(func=_analyze)
