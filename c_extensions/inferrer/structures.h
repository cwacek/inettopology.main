#ifndef STRUCTURES_H_Y78AHTU1
#define STRUCTURES_H_Y78AHTU1


#include "infer.h"
#include <stdexcept>
#include <set>
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>
#include <list>
#include <iterator>


using namespace std;

typedef uint32_t asn_t;
typedef unordered_map<asn_t,int> linkinfo_t;
typedef unordered_map<asn_t,linkinfo_t *> linkdict_t;

class PathSet;

class RQueue {
  public:

    RQueue(redisContext *c,string key,bool am_listener=true);
    ~RQueue();
    string pop();
    void push(string val);
    void clear();

    bool am_listener;
    string key;
    redisContext *c;
    char listener_key[128];

    static const char *add_script; 

    string add_script_sha;

    char k_dolist_set[64], k_dolist_list[64];

};

typedef struct {
  string key;
  string prefix;
  char _cmd[512];
} collection_t;

#define COLLECTION_INIT(name,_key)  \
  collection_t name;\
  name.key = (_key);

#define COLLECTION_CMD_MEMBERS 0x1 << 0

string collection_key(collection_t *c, const char*subkey = 0);

void
collection_set_cmd(collection_t * c, uint32_t cmd, const char *key = 0);

string 
collection_attr(redisContext *c, collection_t *coll,
                string element,
                string attr_key,
                const char*subkey=0);

vector<string> *
collection_members(redisContext *c,collection_t *coll,const char *subkey = 0);

struct VFResult{
  VFResult(bool vf, bool d) { this->vf = vf; missing_data = d;};
  bool vf;
  bool missing_data;
};

class Path {

  public:

    Path();
    Path(vector<string>&);
    Path(string);
    Path(vector<asn_t> &);
    Path(const Path & other);
    //~Path();

    bool equals(const Path &o);

    void init();

    void incr_freq() { frequency++; }
    int ulen() const { return path.size() - sure_count; }
    bool prepend(string elem, bool sure = false);
    bool prepend(asn_t elem, bool sure = false);
    VFResult check_valley_free(linkdict_t &reldict);
    string to_string() const;
    const char * cstr(bool show_uncertain=false);
    size_t cstrlen();

    list<asn_t> path;
    list<asn_t>::const_iterator sp_begin;
    linkinfo_t loop_detect;
    uint32_t sure_count;
    int frequency;
    bool valley_free;
    bool have_loop;
    char cbuf[512];
    bool cbuf_dirty;
    
    bool operator<(const Path &p) const;
  
};

typedef std::shared_ptr<Path> Path_ptr;

struct InitQueueResult {
  PathSet * rib_in;
  set<asn_t> *base_ases;
  InitQueueResult(PathSet * p, set<asn_t> *a) 
  {
    rib_in = p;
    base_ases = a;
  }
};

struct PathPtrCmp {
  bool operator()( const Path_ptr & lhs, const Path_ptr &rhs) const;
};

class PathSet {

  public:
    PathSet();
    ~PathSet();

    typedef set<Path_ptr, PathPtrCmp> pathset_t;
    typedef unordered_map<asn_t,pathset_t>::iterator iterator;

    bool add(asn_t origin, Path_ptr path);
    Path_ptr peek(asn_t origin,bool copy=false);
    void clear(asn_t origin);

    size_t size(asn_t origin);

    unordered_map<asn_t, pathset_t >::iterator begin() 
    {
      return _dict.begin();
    }

    unordered_map<asn_t, pathset_t >::iterator end() 
    {
      return _dict.end();
    }

    unordered_map<asn_t, pathset_t > _dict;
};

#endif /* end of include guard: STRUCTURES_H_Y78AHTU1 */
