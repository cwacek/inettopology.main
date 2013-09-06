#ifndef LOGGER_H_KIZZYBTS
#define LOGGER_H_KIZZYBTS

#include "infer.h"
#include "string.h"

#define LOG_DEBUG 5
#define LOG_INFO 4
#define LOG_NOTICE 3
#define LOG_WARN 2
#define LOG_ERROR 1

using namespace std;

class Logger { 

  public:
    Logger(redisContext *c, string log_key, string id);

    void (Logger::*_log)(int level, const char *msg, va_list ap);
    void redis_log(int level, const char *msg, va_list ap);
    void stderr_log(int level, const char *msg, va_list ap);
    void notice(const char *msg, ...);
    void warn(const char *msg, ...);

    redisContext *c;
    string log_key;
    string id;
};

#endif /* end of include guard: LOGGER_H_KIZZYBTS */
