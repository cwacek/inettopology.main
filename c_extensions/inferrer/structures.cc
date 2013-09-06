#include "structures.h"

RQueue::RQueue(redisContext *c, string key,bool am_listener)
{
  this->key = key;
  this->c = c;
  this->am_listener = am_listener;

  redisReply * r = rCommand(c,"SCRIPT LOAD %b", add_script,strlen(add_script));
  assert(r);
  if (r->type != REDIS_REPLY_STRING) {
    fprintf(stderr,"Error loading script: %s",r->str);
    assert( r->type == REDIS_REPLY_STRING);
  }
  //Save the SHA hash for the script
  add_script_sha = r->str;
  freeReplyObject(r);

  snprintf(listener_key,128,"procqueue:%s:meta:have_listener",key.c_str());
  snprintf(k_dolist_set,64,"procqueue:%s:infilter",key.c_str());
  snprintf(k_dolist_list,64,"procqueue:%s:list",key.c_str());

  if (am_listener) {
    r = rCommand(c,"INCR %s",listener_key);
    assert(r && r->type == REDIS_REPLY_INTEGER);
    freeReplyObject(r);
  }
}

RQueue::~RQueue()
{
  if ( am_listener ) {
    redisReply *r = rCommand(c, "DECR %s",listener_key);
    if (r && r->type == REDIS_REPLY_INTEGER) {
      if (r->integer < 0) {
        fprintf(stderr, "RQueue deletion decremented listener "
                        "keys below zero. Something's fishy.\n");
        freeReplyObject(r);
        r = rCommand(c, "SET %s 0",listener_key);
        freeReplyObject(r);
      }
    } else {
      fprintf(stderr, "Received unexpectedly BAD response "
                      "from Redis. Bailing out.\n");
      assert(0);
    }
  }
}

const char *RQueue::add_script= 
  "local sadd_result = nil; "
  "sadd_result = redis.call('SADD',KEYS[1],ARGV[1]); "
  "if sadd_result > 0 then "
  "  redis.call('LPUSH',KEYS[2],ARGV[1]); "
  "end "
  "return sadd_result; ";

void
RQueue::clear()
{
  redisReply *r = rCommand( c,"DEL %s %s",
                            k_dolist_set,
                            k_dolist_list);

  assert(r && r->type == REDIS_REPLY_INTEGER);
  freeReplyObject(r);
}

void
RQueue::push(string val){
  redisReply *r = rCommand(c, "EVALSHA %s %d %s %s %s",
                               add_script_sha.c_str(),
                               2, k_dolist_set, k_dolist_list,
                               val.c_str());
  assert(r);

  freeReplyObject(r);
}

string 
RQueue::pop() 
{
  string element;
  redisReply *r;

  assert(c && !c->err);
  r = rCommand(c,"BRPOP %s:%s:%s 2","procqueue",key.c_str(),"list");
  if (r->type == REDIS_REPLY_INTEGER) {
    freeReplyObject(r);
    return "";
  }
  else if (r->type == REDIS_REPLY_ARRAY) {
    if (r->elements <= 0) {
      freeReplyObject(r);
      return "";
    }
    element = string(r->element[1]->str);
    freeReplyObject(r);
  }
  else if (r->type == REDIS_REPLY_NIL) {
    freeReplyObject(r);
    return "";
  }

  //Remove it from the set tracker
  r = rCommand(c,"SREM %s:%s:%s %s",
                "procqueue",key.c_str(),"infilter",
                element.c_str());
  assert(r && r->type == REDIS_REPLY_INTEGER && r->integer == 1);
  freeReplyObject(r);

  return element;
}

vector<string> *
collection_members(redisContext *c,collection_t *coll,const char *subkey) 
{
  collection_set_cmd(coll,COLLECTION_CMD_MEMBERS, subkey);
  redisReply *r = rCommand(c,coll->_cmd);
  assert(r && r->type == REDIS_REPLY_ARRAY);

  vector<string> *resp = new vector<string>();

  for (uint32_t i = 0; i < r->elements; i++) {
    resp->push_back(r->element[i]->str);
  }

  freeReplyObject(r);
  return resp;
}

string 
collection_attr(redisContext *c,
                collection_t *coll,
                string element,
                string attr_key,
                const char*subkey)
{
  redisReply *r;
  char key[128];
  snprintf(key,128,
           "collection:%s:attr:%s",
           collection_key(coll,subkey).c_str(),
           element.c_str());

  r= rCommand(c,"HGET %s %s",key,attr_key.c_str());
  assert(r);

  if (r->type == REDIS_REPLY_NIL) {
    freeReplyObject(r);
    return "";
  } 
  else {
    string ret(r->str);
    freeReplyObject(r);
    return ret;
  }
}

string collection_key(collection_t *c, const char*subkey)
{
  if (subkey){
    return c->key + ":" + subkey;
  }
  return c->key;
}

void
collection_set_cmd(collection_t * c, uint32_t cmd, const char *key)
{
  stringstream ss();
  switch (cmd) {
    case COLLECTION_CMD_MEMBERS:
      snprintf(c->_cmd,512,"SMEMBERS collection:%s:set",
               collection_key(c,key).c_str());
      return;
  }

  fprintf(stderr,"Not implemented");
  assert(0);
}

Path::Path(vector<string> &init) 
{
  for (string &x : init) {
    path.push_back(ASN_encode(x));
  }
  this->init();
}

Path::Path(vector<asn_t> &init) 
{
  for (asn_t &x : init) {
    path.push_back(x);
  }
  this->init();
}

Path::Path() 
{
  path.clear();
  init();
}

//Path::~Path()
//{
  //destruct_log("Calling Path destructor");
  //loop_detect.clear();
  //path.clear();
//}

Path::Path(string pathstring) 
{
  string delim = ",[]' ";
  string::size_type lastPos = pathstring.find_first_not_of(delim,0);
  string::size_type pos = pathstring.find_first_of(delim,lastPos);

  while (string::npos != pos || string::npos != lastPos) {
    path.push_back(ASN_encode(pathstring.substr(lastPos,pos - lastPos)));
    lastPos = pathstring.find_first_not_of(delim,pos);
    pos = pathstring.find_first_of(delim,lastPos);
  }

  init();
}

bool
Path::equals(const Path &o)
{
  return (!(*this < o) && !(o < *this));
}

Path::Path(const Path &o)
{
  sure_count = o.sure_count;
  for (asn_t x : o.path) {
    path.push_back(x);
  }
  loop_detect = o.loop_detect;
  frequency = o.frequency;
  have_loop = o.have_loop;
  valley_free = o.valley_free;
  cbuf_dirty = true;

  sp_begin = path.begin();
  for (uint32_t i = 0; i < path.size() - sure_count ; i++)
    sp_begin++;
}

void
Path::init()
{
  sure_count = path.size();
  frequency = 1;
  valley_free = true;
  have_loop = false;
  cbuf_dirty = true;
  cbuf[0] = '\0';
  for (asn_t &x : path) {
    loop_detect[x] = 1;
  }
  sp_begin = path.begin();
}

bool
Path::prepend(string elem, bool sure) {
  return prepend(ASN_encode(elem),sure);
}

bool
Path::prepend(asn_t elem, bool sure) {
  
  linkinfo_t::iterator it;
  it = loop_detect.find(elem);

  if ( it != loop_detect.end() ) {
    have_loop = true;
    return false;
  } 
  else {
    //Add to loop detection
    loop_detect.insert(pair<asn_t,int>(elem,1));
  }

  path.push_front(elem);
  if (sure) {
    sure_count++;
    sp_begin--;
  }

  cbuf_dirty = true;
  return true;
}

size_t
Path::cstrlen()
{
  if (cbuf_dirty)
    cstr();
  return strnlen(cbuf,512);
}

const char *
Path::cstr(bool show_uncertain) 
{
  int i = 0;
  char * ins = (char *)cbuf;

  if (cbuf_dirty) {
    for (asn_t x : path) {
      const char *fmt;
      if (show_uncertain && i < ulen()) {
        fmt = "[%s] ";
      } 
      else {
        fmt = "%s ";
      }
      ins += snprintf(ins,sizeof(cbuf)-(ins-cbuf),
                      fmt,ASN_decode(x));
      i++;
    }
    *(ins-1) = '\0';
    cbuf_dirty = false;
  }

  return cbuf;
}


string
Path::to_string()  const
{
  stringstream iss;
  for (asn_t x : path) {
    iss << ASN_decode(x) << " ";
  }
  return iss.str();
}

VFResult
Path::check_valley_free(linkdict_t &reldict) 
{
  list<asn_t>::iterator as1_it, as2_it;
  as1_it = path.begin();
  as2_it = path.begin();
  as2_it++;
  
  if (as1_it == sp_begin)
    return VFResult(true,false);

#define DOWN -1
#define UP 1
#define NONE 0

  int direction = NONE;
  int relation;

  while (as1_it != sp_begin) {
    try {
      linkinfo_t * as1_links = reldict.at(*as1_it); 
      relation = as1_links->at(*as2_it);
    }
    catch (out_of_range &oor) {
      return VFResult(false,true);
    }

    if (direction == NONE) {
      switch (relation) {
        case AS_REL_PEER:
        case AS_REL_CUSTOMER:
          direction = DOWN;
          break;
        case AS_REL_PROVIDER:
          direction = UP;
          break;
      }
    } else if (direction == DOWN) {
      switch (relation) {
        case AS_REL_PROVIDER:
        case AS_REL_PEER:
          return VFResult(false,false);
      }
    } else if (direction == UP) {
      switch (relation) {
        case AS_REL_PEER:
        case AS_REL_CUSTOMER:
          direction = DOWN;
      }
    }

    as2_it++;
    as1_it++;
  }

  return VFResult(true,false);

#undef UP
#undef DOWN
#undef NONE
}

bool
Path::operator<(const Path &p) const 
{
  if (path.size() != p.path.size())
    return path.size() < p.path.size();
  if (ulen() != p.ulen())
    return ulen() < p.ulen();
  if (frequency != p.frequency)
    return frequency > p.frequency;
  if (path.front() < p.path.front())
    return true;

  return false;
}

PathSet::PathSet() 
{
  
}

PathSet::~PathSet()
{
  destruct_log("Calling PathSet destructor");
  for ( auto x : _dict) {
    destruct_log("Pathset destructing ASID %d", x.first);
    for (Path_ptr p : x.second) {
      p.reset();
    }
  }
}

/* Add an path to the pathset. If the path was already in the 
 * pathset, then increment its frequency and return false.
 *
 * If the path was not previously in the pathset, return true.
 **/
bool
PathSet::add(asn_t origin, Path_ptr path)
{
  unordered_map<asn_t, pathset_t >::iterator search_it;
  pathset_t origin_rib_in;

  set<Path_ptr>::iterator it = _dict[origin].find(path);
  Path_ptr p;
  // If this element is in the set already, we need
  // to remove it and re-add it with a higher frequency
  // because the set won't reorder if we just change
  // it's frequency while stored.

  if (it != _dict[origin].end()) {
    p = *it;
    _dict[origin].erase(it);
    p->incr_freq();
    _dict[origin].insert(p);
    return false;
  }
  else {
    _dict[origin].insert(path);
    return true;
  }
}

size_t
PathSet::size(asn_t origin)
{
  unordered_map<asn_t, pathset_t >::iterator search_it;

  search_it = _dict.find(origin);
  if (search_it == _dict.end()) 
    return 0;

  return search_it->second.size();
}

bool 
PathPtrCmp::operator()( const Path_ptr &lhs, const Path_ptr &rhs) const
{
  return ( (*lhs) < (*rhs) );
}

void 
PathSet::clear(asn_t origin)
{
  unordered_map<asn_t, pathset_t >::iterator search_it;

  search_it = _dict.find(origin);
  if (search_it != _dict.end()) {
    _dict.erase(search_it);
  }
}

Path_ptr
PathSet::peek(asn_t origin, bool copy)
{
  unordered_map<asn_t, pathset_t >::iterator search_it;

  search_it = _dict.find(origin);
  if (search_it == _dict.end()) 
    return Path_ptr();
  
  pathset_t pathset = search_it->second;
  if (pathset.size() == 0) {
    return Path_ptr();
  }

  Path_ptr p;
  if (!copy) {
    pathset_t::iterator it = pathset.begin();
    p = *it;
  }
  else {
    pathset_t::iterator it = pathset.begin();
    p = Path_ptr(new Path(**it));
    mem_debug("Initialized new Path @%p",p);
  }
  return p;
}
