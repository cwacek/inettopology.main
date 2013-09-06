#include <gtest/gtest.h>
#include "../structures.h"
#include "../infer.h"



class RQueueTest: public ::testing::Test 
{
  protected:

    static void SetUpTestCase() {
      c_ = redisConnect("127.0.0.1",
                       6379); 
    }

    static void TearDownTestCase() {
      delete c_;
      c_ = NULL;
    }

    virtual void SetUp() {
      _rqueue = new RQueue(c_,testing,true);
    }

    virtual void TearDown() {
      if (c_ && !c_->err) { 
        r = rCommand(c_,"DEL procqueue:%s:infilter procqueue:%s:list",
                     testing.c_str(),testing.c_str());
        freeReplyObject(r);
      }
      r = rCommand(c_,"DEL testing:list testing:list2");
      freeReplyObject(r);
      _rqueue->clear();
      delete _rqueue;
    }

    RQueue *_rqueue;
    static redisContext *c_;
    redisReply *r;
    static const string testing;
};

redisContext *RQueueTest::c_;

const string RQueueTest::testing = "testingqueue2348lskajfl";

#define ASSERT_REDIS(c) \
  ASSERT_TRUE((c) != NULL) \
      << "Don't have a Redis connection. Expected " \
      << "one on 'localhost:6379'"

#define ASSERT_REDIS_INT(r,exp) \
  ASSERT_TRUE((r) != NULL); \
  ASSERT_EQ(REDIS_REPLY_INTEGER,(r)->type); \
  ASSERT_EQ(exp, (r)->integer)

TEST_F(RQueueTest, HaveRedis)
{
  ASSERT_TRUE(c_ != NULL)
      << "Don't have a Redis connection. Expected "
      << "one on 'localhost:6379'";
  ASSERT_EQ(0, c_->err) 
      << "Don't have a Redis connection. Expected "
      << "one on 'localhost:6379'";
}

TEST_F(RQueueTest, NewRQueueSetsExistence)
{
  ASSERT_REDIS(c_);

  r = rCommand(c_,"EXISTS %s",_rqueue->listener_key);
  ASSERT_REDIS_INT(r, 1);
  freeReplyObject(r);

  r = rCommand(c_,"SCRIPT EXISTS %s",_rqueue->add_script_sha.c_str());
  ASSERT_EQ(REDIS_REPLY_ARRAY,r->type);
  ASSERT_EQ(1,r->elements);
  ASSERT_EQ(1,r->element[0]->integer);
}

TEST_F(RQueueTest, DeleteRemovesListenerKey)
{
  ASSERT_REDIS(c_);

  RQueue *rq = new RQueue(c_,"sladfkjew",false);
  char buf[64];
  strncpy(buf,rq->listener_key,64);
  r = rCommand(c_,"GET %s",buf);
  int res = atoi(r->str);
  ASSERT_EQ(1,res);
  freeReplyObject(r);

  delete rq;
  r = rCommand(c_,"GET %s", buf);
  res = atoi(r->str);
  ASSERT_EQ(1,res);
  freeReplyObject(r);
}

TEST_F(RQueueTest, BRPOPBlocksCorrectly)
{
  ASSERT_REDIS(c_);

  r = rCommand(c_,"DEL testing:list");
  freeReplyObject(r);

  r = rCommand(c_,"BRPOP testing:list 2");
  ASSERT_EQ(REDIS_REPLY_NIL,r->type);
  freeReplyObject(r);

  r = rCommand(c_,"LPUSH testing:list hello");
  ASSERT_REDIS_INT(r,1)
    << "LPUSH returning incorrect number of items in list.";
  freeReplyObject(r);

  r = rCommand(c_,"BRPOP testing:list 2");
  ASSERT_TRUE(r != NULL);
  ASSERT_EQ(REDIS_REPLY_ARRAY,r->type );
  ASSERT_EQ(2,r->elements);
  ASSERT_STREQ("hello",r->element[1]->str);
  freeReplyObject(r);

  for (int i = 0; i < 100; i++)
  {
    r = rCommand(c_,"LPUSH testing:list2 tmp");
    ASSERT_REDIS_INT(r,i+1);
    freeReplyObject(r);
  }

  for (int i = 0; i < 100; i++) {
    r = rCommand(c_, "BRPOP testing:list2 2");
    ASSERT_EQ(REDIS_REPLY_ARRAY,r->type )
      << "Failed BRPOP on iteration " << i;
    ASSERT_EQ(2,r->elements);
    freeReplyObject(r);
  }

  r = rCommand(c_,"LPUSH testing:list hello");
  ASSERT_REDIS_INT(r,1);
  freeReplyObject(r);

  r = rCommand(c_,"BRPOP testing:list 2");
  ASSERT_TRUE(r != NULL);
  ASSERT_EQ(REDIS_REPLY_ARRAY,r->type );
  ASSERT_EQ(2,r->elements);
  ASSERT_STREQ("hello",r->element[1]->str);
  freeReplyObject(r);

}

TEST_F(RQueueTest, EmptyReturnsEmptyString)
{
  ASSERT_TRUE(c_ != NULL)
      << "Don't have a Redis connection. Expected "
      << "one on 'localhost:6379'";


  string res = _rqueue->pop();

  ASSERT_STREQ("",res.c_str()); 
}

TEST_F(RQueueTest, InsertAddsToQueue)
{
  ASSERT_TRUE(c_ != NULL)
      << "Don't have a Redis connection. Expected "
      << "one on 'localhost:6379'";

  _rqueue->push("winner");

  r = rCommand(c_,"SCARD %s",_rqueue->k_dolist_set);
  ASSERT_REDIS_INT(r,1);
  freeReplyObject(r);
  r = rCommand(c_,"LLEN %s",_rqueue->k_dolist_list);
  ASSERT_REDIS_INT(r,1);
  freeReplyObject(r);

  _rqueue->push("tesla");

  r = rCommand(c_,"SCARD %s",_rqueue->k_dolist_set);
  ASSERT_REDIS_INT(r,2);
  freeReplyObject(r);
  r = rCommand(c_,"LLEN %s",_rqueue->k_dolist_list);
  ASSERT_REDIS_INT(r,2);
  freeReplyObject(r);
}

TEST_F(RQueueTest, DuplicateInsertsIgnored)
{
  ASSERT_TRUE(c_ != NULL)
      << "Don't have a Redis connection. Expected "
      << "one on 'localhost:6379'";


  /** Insert 1 **/
  _rqueue->push("winner");

  r = rCommand(c_,"SCARD %s",_rqueue->k_dolist_set);
  ASSERT_TRUE(r != NULL);
  ASSERT_EQ(REDIS_REPLY_INTEGER,r->type);
  ASSERT_EQ(1,r->integer);
  freeReplyObject(r);
  r = rCommand(c_,"LLEN %s",_rqueue->k_dolist_list);
  ASSERT_TRUE(r != NULL);
  ASSERT_EQ(REDIS_REPLY_INTEGER,r->type);
  ASSERT_EQ(1,r->integer);
  freeReplyObject(r);

  /** Insert 2 **/
  _rqueue->push("winner");
  r = rCommand(c_,"SCARD %s",_rqueue->k_dolist_set);
  ASSERT_TRUE(r != NULL);
  ASSERT_EQ(REDIS_REPLY_INTEGER,r->type);
  ASSERT_EQ(1,r->integer);
  freeReplyObject(r);
  r = rCommand(c_,"LLEN %s",_rqueue->k_dolist_list);
  ASSERT_TRUE(r != NULL);
  ASSERT_EQ(REDIS_REPLY_INTEGER,r->type);
  ASSERT_EQ(1,r->integer);
  freeReplyObject(r);
}

TEST_F(RQueueTest, PopReturnsInOrder)
{

  ASSERT_TRUE(c_ != NULL)
      << "Don't have a Redis connection. Expected "
      << "one on 'localhost:6379'";

  _rqueue->push("winner");
  _rqueue->push("loser");

  string res = _rqueue->pop();
  ASSERT_STREQ("winner",res.c_str());
  res = _rqueue->pop();
  ASSERT_STREQ("loser",res.c_str());
  res = _rqueue->pop();
  ASSERT_STREQ("",res.c_str());
}

TEST_F(RQueueTest, InsertPopInsert)
{
  ASSERT_TRUE(c_ != NULL)
      << "Don't have a Redis connection. Expected "
      << "one on 'localhost:6379'";

  _rqueue->push("winner");
  string res = _rqueue->pop();
  ASSERT_STREQ("winner",res.c_str());

  r = rCommand(c_,"SCARD %s", _rqueue->k_dolist_set);
  ASSERT_EQ(REDIS_REPLY_INTEGER,r->type);
  ASSERT_EQ(0,r->integer);

  _rqueue->push("winner");
  res = _rqueue->pop();
  ASSERT_STREQ("winner",res.c_str());

  r = rCommand(c_,"SCARD %s", _rqueue->k_dolist_set);
  ASSERT_EQ(REDIS_REPLY_INTEGER,r->type);
  ASSERT_EQ(0,r->integer);
}

TEST_F(RQueueTest, ClearEmptiesList)
{
  ASSERT_TRUE(c_ != NULL)
      << "Don't have a Redis connection. Expected "
      << "one on 'localhost:6379'";

  _rqueue->push("winner");
  _rqueue->push("loser");

  _rqueue->clear();
  string res = _rqueue->pop();
  ASSERT_STREQ("",res.c_str());
}
