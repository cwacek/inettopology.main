#include "infer.h"
#include "cmdline.h"

using namespace std;

int main(int argc, char const *argv[])
{
  gengetopt_args_info args;

  if (cmdline_parser(argc, (char **)argv,&args) != 0)
    exit(1);


  redisContext *c = redisConnect(args.redis_host_arg,
                                 args.redis_port_arg);

  if (!c || c->err)
  {
    fprintf(stderr,"Error: %s\n",c->errstr);
    exit(1);
  }

  FLAGS_INIT(flags);
  if (args.dump_graph_flag) {
    FLAG_SET(flags, FLAG_DUMP_GRAPH);
  }

  known_path(c,string(args.procqueue_arg),string(args.ribtag_arg), flags);

  return 0;
}
