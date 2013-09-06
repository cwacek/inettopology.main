#include "logger.h"

Logger::Logger(redisContext *c, string log_key, string id)
{
  this->c = c;
  this->log_key = log_key;
  this->id = id;

  redisReply *r;
  r = rCommand(this->c, "EXISTS logsink:%s:operate", log_key.c_str());
  if (!r || r->type != REDIS_REPLY_INTEGER || r->integer == 0) {
    fprintf(stderr, "No logsink appears to be established for this log "
                    "stream. Falling back to stderr.\n");

    _log = &Logger::stderr_log;
  }
  else {
    _log = &Logger::redis_log;
  }
  if (r)
    freeReplyObject(r);
}

void Logger::stderr_log(int level,const char *msg, va_list ap)
{
  char fmt_msg[512];
  char *msg_offset = fmt_msg;
  const char *fmt_str;
  switch (level) {
    case LOG_DEBUG:
      fmt_str = "%d:%s:LOG_DEBUG::";
      break;
    case LOG_INFO:
      fmt_str = "%d:%s:LOG_INFO::";
      break;
    case LOG_WARN:
      fmt_str = "%d:%s:LOG_WARN::";
      break;
    case LOG_NOTICE:
      fmt_str = "%d:%s:LOG_NOTICE::";
      break;
    case LOG_ERROR:
      fmt_str =  "%d:%s:LOG_ERROR::";
      break;
    default:
      return;
  }

  msg_offset += snprintf(msg_offset, 
                        sizeof(fmt_msg) - (msg_offset-fmt_msg),
                        fmt_str,
                        time(0),
                        this->id.c_str());

  msg_offset += vsnprintf(msg_offset,
                          sizeof(fmt_msg) - (msg_offset-fmt_msg),
                          msg, ap);

  fprintf(stderr,"%s\n",fmt_msg);
}

void Logger::redis_log(int level,const char *msg, va_list ap)
{
  char fmt_msg[512];
  char *msg_offset = fmt_msg;
  const char *fmt_str;

  switch (level) {
    case LOG_DEBUG:
      fmt_str = "%d:%s:LOG_DEBUG::";
      break;
    case LOG_INFO:
      fmt_str = "%d:%s:LOG_INFO::";
      break;
    case LOG_WARN:
      fmt_str = "%d:%s:LOG_WARN::";
      break;
    case LOG_NOTICE:
      fmt_str = "%d:%s:LOG_NOTICE::";
      break;
    case LOG_ERROR:
      fmt_str =  "%d:%s:LOG_ERROR::";
      break;
    default:
      return;
  }

  msg_offset += snprintf(msg_offset, 
                        sizeof(fmt_msg) - (msg_offset-fmt_msg),
                        fmt_str,
                        time(0),
                        this->id.c_str());

  msg_offset += vsnprintf(msg_offset,
                          sizeof(fmt_msg) - (msg_offset-fmt_msg),
                          msg, ap);

  redisReply *r;
  r = rCommand(this->c, "LPUSH logger:%s %b",
                        log_key.c_str(),
                        fmt_msg, strnlen(fmt_msg,512));
  assert(r);
  freeReplyObject(r);
}

void Logger::warn(const char * msg, ...) {
  va_list ap;
  va_start(ap,msg);
  (this->*_log)(LOG_WARN,msg,ap);
  va_end(ap);
}

void Logger::notice(const char * msg, ...) {
  va_list ap;
  va_start(ap,msg);
  (this->*_log)(LOG_NOTICE,msg,ap);
  va_end(ap);
}
