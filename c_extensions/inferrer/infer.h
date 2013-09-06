#ifndef INFER_H_SDJMQITX
#define INFER_H_SDJMQITX

#include <assert.h>
#include <time.h>
#include "hiredis/hiredis.h"
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <unistd.h>
#include <bitset>
#include <string>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include "logger.h"
#include "structures.h"
               
#define AS_REL_UNKNOWN 5
#define AS_REL_SIBLING 0
#define AS_REL_CUSTOMER -1
#define AS_REL_PROVIDER 1
#define AS_REL_PEER 2

#define rCommand(c,cmd,...) (redisReply *) redisCommand((c),(cmd),##__VA_ARGS__)

#define FLAG_DUMP_GRAPH 1

#define FLAGS_INIT(flags) int flags = 0
#define FLAG_SET(flags,flagname) flags |= (0x1 << ( flagname ))
#define FLAG_UNSET(flags,flagname)  flags &= ~(0x1 << ( flagname ))
#define FLAG_GET(flags,flagname) ( ( flags ) & (0x1 << ( flagname )) )

#undef MEM_DEBUG
#ifdef MEM_DEBUG
#define mem_debug(string,...) \
  fprintf(stderr, string "\n", ##__VA_ARGS__)
#else
#define mem_debug(string,...) ;
#endif

#undef DESTRUCT_DEBUG
#ifdef DESTRUCT_DEBUG
#define destruct_log(string,...) \
  fprintf(stderr, string "\n", ##__VA_ARGS__)
#else
#define destruct_log(string, ...) ;
#endif

void known_path(redisContext *c, string nametag, string ribtag, int flags);

typedef uint32_t asn_t;

asn_t
ASN_encode(string ASN);

const char *
ASN_decode(asn_t ASN);

const char *
ASN_decode_new(asn_t ASN);


#endif /* end of include guard: INFER_H_SDJMQITX */
