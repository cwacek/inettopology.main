#include "infer.h"

using namespace std;

string
get_relation(redisContext *c, string AS, string AS2); 

InitQueueResult
init_active_queue(redisContext *c,
                  set<asn_t> &queue,
                  string dest,
                  string ribtag,
                  Logger &log);

struct statratio {
  int subctr;
  int total;
  statratio() { subctr = 0; total = 0;}
  statratio& operator+=(const statratio &o) 
  {
    subctr += o.subctr;
    total += o.total;
    return *this;
  }
};

asn_t
ASN_encode(string ASN)
{
  asn_t result;
  size_t loc;
  if ((loc = ASN.find_first_of(".")) != string::npos)
  {
    result = 500000;
    result += (10000 * atoi(ASN.substr(0,loc).c_str()));
    string substr = ASN.substr(loc+1);
    switch (substr.size()) {
      case 1:
        //thousands
        result += atoi(substr.c_str()) * 1000;
        break;
      case 2:
        //thousands
        result += atoi(substr.c_str()) * 100;
        break;
      case 3:
        //thousands
        result += atoi(substr.c_str()) * 10;
        break;
      case 4:
        result += atoi(substr.c_str());
        break;
      default:
        assert(0);
    }

    return result;
  }
  result = atoi(ASN.c_str());
  assert(result);
  return result;
}

void
__ASN_decode(asn_t ASN, char*dst, size_t len)
{
  if (ASN > 500000) {
    ASN -= 500000;
    snprintf(dst, len,
            "%u.%04u", ASN/10000, (ASN%10000));
  }
  else {
    snprintf(dst, len,
            "%u",ASN);
  }
}

char _asn_decoder_buf[32];

const char*
ASN_decode(asn_t ASN)
{
  __ASN_decode(ASN,_asn_decoder_buf,sizeof(_asn_decoder_buf));
  return _asn_decoder_buf;
}

const char* 
ASN_decode_new(asn_t ASN)
{
  char *buf = new char[32];
  __ASN_decode(ASN,buf,32);
  return buf;
}

/** Add links from the collection *rlinks* to the 
 *  map *links*.
 *
 *  @param bool convert translate AS numbers into 
 *                      integers
 **/
statratio
add_links_to_dict(redisContext *c, 
                  collection_t *rlinks,
                  linkinfo_t * links,
                  asn_t AS) 
{
  vector<string> *peers = collection_members(c,rlinks,ASN_decode(AS));
  statratio stats;

  vector<string>::iterator peer_it = peers->begin();
  for (; peer_it != peers->end(); peer_it++) {
    if ((*peer_it).find_first_of('.') != string::npos) {
      continue; // We don't want to consider those odd 3.122 ASes
    }
    string relation = get_relation(c,ASN_decode(AS),*peer_it);
    asn_t encoded_AS = ASN_encode(*peer_it);
    if (relation == "p2p") { 
      links->insert(pair<asn_t,int>(encoded_AS, AS_REL_PEER));
    } 
    else if (relation == "p2c") {
      links->insert(pair<asn_t,int>(encoded_AS, AS_REL_CUSTOMER));
    }
    else if (relation == "c2p") {
      links->insert(pair<asn_t,int>(encoded_AS, AS_REL_PROVIDER));
    }
    else if (relation == "sibling") {
      links->insert(pair<asn_t,int>(encoded_AS, AS_REL_SIBLING));
    }
    else {
      stats.subctr++;
    }
    stats.total++;
  }

  delete peers;
  return stats;
}

void 
add_members_to_set(redisContext *c, collection_t *collection, set<asn_t> & dest)
{
  collection_set_cmd(collection,COLLECTION_CMD_MEMBERS);
  redisReply *r = rCommand(c,collection->_cmd);

  for (uint32_t i = 0; i < r->elements; i++) {
    redisReply *elem = r->element[i];
    if (elem->type == REDIS_REPLY_STRING) {
      if (strchr(elem->str,'.') != 0 )
        continue; // Don't consider weird 3.22 ASES
      dest.insert(ASN_encode(elem->str));
    }
  }

  freeReplyObject(r);
}  

string
get_relation(redisContext *c, string AS, string AS2) {

  redisReply *r = rCommand(c,"HGET as:%s:rel %s",AS.c_str(),AS2.c_str());

  if (r->type == REDIS_REPLY_NIL) {
    freeReplyObject(r);
    return "";
  }
  else if (r->type == REDIS_REPLY_STRING) {
    string ret(r->str);
    freeReplyObject(r);
    return ret;
  }
  assert(0);
}

asn_t
get_candidate(set<asn_t> &candidateQueue)
{
  asn_t candidate = *(candidateQueue.begin());
  candidateQueue.erase(candidateQueue.begin());
  return candidate;
}

bool
ribtag_exists(redisContext *c, string ribtag)
{
  redisReply *r;
  r = rCommand(c,"EXISTS collection:%s_ases:set",ribtag.c_str());
  bool result;

  if (r && r->type == REDIS_REPLY_INTEGER && r->integer == 1) {
    result =true;
  }
  else {
    result = false;
  }
  if (r)
    freeReplyObject(r);
  return result;
}

void known_path(redisContext *c, string nametag, string ribtag, int flags)
{
  assert(c);
  char pidbuf[64];
  snprintf(pidbuf,sizeof(pidbuf),"%u_%s",getpid(),ribtag.c_str());

  Logger log = Logger(c,"route_inference",pidbuf);
  log.notice("Starting up");

  RQueue destinationQueue = RQueue(c,nametag,true);
  //destinationQueue.clear();
  COLLECTION_INIT(as_set,"base_ases");
  COLLECTION_INIT(rib_as_set,ribtag + "_ases")

  if (!ribtag_exists(c,ribtag)) {
    fprintf(stderr,
            "Inferrer for %s shutting down because "
            "no RIB data was available for the given tag.\n",
            ribtag.c_str());
    return;
  }

  pair<set<asn_t>::iterator,bool> ret;
  set<asn_t> all_ases;

  add_members_to_set(c,&as_set,all_ases);
  add_members_to_set(c,&rib_as_set,all_ases);

  log.notice("Loaded AS Set [%u ases]",all_ases.size());

  if (FLAG_GET(flags,FLAG_DUMP_GRAPH)) {
    set<asn_t>::iterator as_it = all_ases.begin();
    for (; as_it != all_ases.end(); as_it++) {
      destinationQueue.push(ASN_decode(*as_it));
    }
  }

  log.notice("Loading link structure data...");

  linkdict_t as_links;
  COLLECTION_INIT(as_rel_keys,"as_rel_keys");
  COLLECTION_INIT(base_as_links,"base_as_links");
  COLLECTION_INIT(rib_as_links,ribtag + "_as_links");

  statratio skipped;
  set<asn_t>::iterator as_iter = all_ases.begin();
  for (; as_iter != all_ases.end(); as_iter++) {
    asn_t AS = *as_iter;
    linkinfo_t *thisASlinks = new linkinfo_t();
    as_links[AS] = thisASlinks;

    skipped += add_links_to_dict(c,&base_as_links,thisASlinks,AS);
    skipped += add_links_to_dict(c,&rib_as_links,thisASlinks,AS);
  }
  log.notice("Link structure loading done. %d/%d skipped "
             "because no AS relationship data was available.",
             skipped.subctr, skipped.total);

  set<asn_t> candidate_queue;
  while (1) {
    time_t dest_timer = time(0);

    fprintf(stderr,"Looking for new destination\n");
    string dest = destinationQueue.pop();
    if (dest.empty()) {
      log.notice("Waiting for elements to process...");
      continue;
    }
    log.notice("Processing request for routes to %s",dest.c_str());

    candidate_queue.clear();
    InitQueueResult iqr = init_active_queue(c,candidate_queue,dest,ribtag,log);
    PathSet *rib_in = iqr.rib_in;
    set<asn_t> *base_ases = iqr.base_ases;

    if (candidate_queue.size() == 0) {
      log.warn("No known routes to %s",dest.c_str());
      redisReply *r = rCommand(c,"PUBLISH inference:query_status %s|%s",ribtag.c_str(),dest.c_str());
      freeReplyObject(r);
      continue;
    }

    while (candidate_queue.size() > 0) {
      //fprintf(stderr,"looping through candidates\n");
      int candidate = get_candidate(candidate_queue);

      //log.notice("Trying new candidate %s. %u remain in queue.",
                 //ASN_decode(candidate),candidate_queue.size());

      linkdict_t::iterator outer_iter;
      linkinfo_t::iterator peer_iter; 
      outer_iter = as_links.find(candidate);
      if (outer_iter == as_links.end()) {
        log.notice("Couldn't find any links for %s", ASN_decode(candidate));
        continue;
      }

      peer_iter = outer_iter->second->begin();
      for (; peer_iter != outer_iter->second->end(); peer_iter++) {
        asn_t peer = peer_iter->first;

        //Skip ones belonging to the base AS
        if (base_ases->find(peer) != base_ases->end())
          continue;

        Path_ptr best_candidate = rib_in->peek(candidate,true);
        mem_debug("Path returned from peek has address %p", best_candidate);
        //log.notice("Considering adding %s to '%s'",
                   //peer.c_str(),
                   //best_candidate->cstr());
        bool loop_free = best_candidate->prepend(peer);

        if (!loop_free)  {
          //log.notice("Candidate rejected because of loop.");
          mem_debug("Deleting path @%p",best_candidate);
          best_candidate.reset(); //shared ptrs do this
          continue;
        }

        VFResult vfr = best_candidate->check_valley_free(as_links);
        if (!vfr.vf) {
          //log.notice("Candidate rejected because of valley-freeness "
                     //"violations. Data problems? %s",
                     //(vfr.missing_data ? "yes" :"no"));
          mem_debug("Deleting path @%p",best_candidate);
          best_candidate.reset(); //shared ptrs do this
          continue;
        }

        Path_ptr tmp_path = rib_in->peek(peer);
        bool referenced =  rib_in->add(peer,best_candidate);
        if (!tmp_path ||  !(rib_in->peek(peer)->equals(*tmp_path))) {
          //log.notice("Adding new candidate starting point '%s' because "
                     //"it got a better path. Old: [%s] New: [%s]",
                     //peer.c_str(),
                     //(tmp_path) ? ((Path *)tmp_path)->cstr() : "",
                     //best_candidate->cstr());
          candidate_queue.insert(peer);
        }
        if (!referenced) {
          mem_debug("Deleting path @%p",best_candidate);
          best_candidate.reset(); //shared ptrs do this
        }
      }
    }

    PathSet::iterator origin_it;
    origin_it = rib_in->begin();
    redisReply *r; 
    char result_key[128];
    snprintf(result_key, 128,
             "result:%s:inferred_to:%s",
             ribtag.c_str(),dest.c_str());

    /** Setup the argv for the redis command **/
    const char *args[4];
    size_t arglen[4];
    args[0] = "HSET";
    arglen[0] = 4;
    args[1] = result_key;
    arglen[1] = strnlen(result_key,sizeof(result_key));

    int cmd_ctr = 0, total_ctr = 0;
    for (; origin_it != rib_in->end(); origin_it++) {
      PathSet::pathset_t::iterator bpath_it = origin_it->second.begin();
      Path_ptr p = *bpath_it;
      if (FLAG_GET(flags,FLAG_DUMP_GRAPH)) {
        fprintf(stdout, "%s\n", p->cstr());
        continue;
      }
      if (cmd_ctr < 100) {

        args[2] = ASN_decode_new(origin_it->first);
        arglen[2] = strnlen(args[2],sizeof(_asn_decoder_buf));
        args[3] = p->cstr();
        arglen[3] = p->cstrlen();

        redisAppendCommandArgv(c, 4, args, arglen); 
        delete [] args[2];
        cmd_ctr++;
        total_ctr++;
      } 
      else {
        for (int _i = 0; _i < cmd_ctr; _i++) {
          redisGetReply(c,(void **)&r);
          freeReplyObject(r);
        }
        cmd_ctr = 0;
      }
    }
    if (! FLAG_GET(flags,FLAG_DUMP_GRAPH)) {
      for (int _i = 0; _i < cmd_ctr; _i++) {
        if (redisGetReply(c,(void **)&r) != REDIS_OK) {
          fprintf(stderr, "Error getting reply from pipelined cmd on iteration %d", 
                  _i );
        }
        freeReplyObject(r);
      }
      r = rCommand(c,"PUBLISH inference:query_status %s|%s",
                   ribtag.c_str(),dest.c_str());
      freeReplyObject(r);
      r = rCommand(c,"EXPIRE %s 600",result_key);
      freeReplyObject(r);
    }
    log.notice("Inferred Routes to %s. Took %u seconds",
               dest.c_str(),
               time(0)-dest_timer);

    delete rib_in;
    delete base_ases;
  }

  as_links.clear();
}

InitQueueResult
init_active_queue(redisContext *c,
                  set<asn_t> &queue,
                  string dest,
                  string ribtag,
                  Logger &log)
{
  PathSet *rib_in = new PathSet();
  set<asn_t> *base_ases = new set<asn_t>();
  int ctr = 0;
  set<asn_t> rib_ases;

  char sure_path_key[48];
  snprintf(sure_path_key,48,"sure_path_to:%s",dest.c_str());

  COLLECTION_INIT(rib_as_set,ribtag +"_ases");
  add_members_to_set(c,&rib_as_set,rib_ases);

  set<asn_t>::iterator as_iter = rib_ases.begin();
  for (; as_iter != rib_ases.end(); as_iter++) {
    string sure_path = collection_attr(c,
                                       &rib_as_set,
                                       ASN_decode(*as_iter),
                                       sure_path_key);

    if (!sure_path.empty()){
      Path_ptr p = Path_ptr(new Path(sure_path));
      if (p) {
        queue.insert(*as_iter);
        rib_in->add(*as_iter,p);
        base_ases->insert(*as_iter);
        ctr++;
      }
    }
  }

  log.notice("%d/%u ASes have sure paths to %s",
              ctr,rib_ases.size(),dest.c_str());

  return InitQueueResult(rib_in,base_ases);
}



