#include <gtest/gtest.h>
#include "../structures.h"

TEST(PathTest, EmptyNew) 
{
  Path * p = new Path();
  ASSERT_EQ(p->sure_count,p->path.size());
  ASSERT_EQ(1, p->frequency);
  ASSERT_TRUE(p->valley_free);
  ASSERT_FALSE(p->have_loop);
  ASSERT_EQ(0,p->ulen());
  ASSERT_EQ(p->path.end(),p->sp_begin);
  delete p;
}

TEST(PathTest, FromStringVector) 
{
  std::vector<std::string> x;
  x.push_back("1234");
  x.push_back("53224");
  Path * p = new Path(x);
  ASSERT_STREQ("1234 53224",p->cstr());
  ASSERT_EQ(p->path.begin(),p->sp_begin);
  delete p;
}

TEST(PathTest, FromIntVector) 
{
  std::vector<asn_t> x;
  x.push_back(ASN_encode("1234"));
  x.push_back(ASN_encode("5.3224"));
  Path * p = new Path(x);
  ASSERT_STREQ("1234 5.3224",p->cstr());
  ASSERT_EQ(p->path.begin(),p->sp_begin);
  delete p;
}

TEST(PathTest, FromString)
{
  std::string input = "['1234', '23454', '332345']";
  Path *p = new Path(input);
  ASSERT_STREQ("1234 23454 332345",p->cstr());
  ASSERT_EQ(p->path.begin(),p->sp_begin);
}

TEST(PathTest, PrependAddsToFront)
{
  std::string input = "['1234', '23454', '332345']";
  Path *p = new Path(input);
  bool succ = p->prepend("23442",false);
  ASSERT_STREQ("23442 1234 23454 332345",p->cstr());
  ASSERT_EQ(1,p->ulen());
  ASSERT_TRUE(succ);
  list<asn_t>::iterator t = p->path.begin();
  t++;
  ASSERT_EQ(t,p->sp_begin);
  delete p;
}

TEST(PathTest, PrependSureIncrSureLen)
{
  std::string input = "['1234', '23454', '332345']";
  Path *p = new Path(input);
  int surelen = p->sure_count;
  int ulen = p->ulen();

  bool succ = p->prepend("23442",true);
  ASSERT_EQ(surelen +1, p->sure_count);
  ASSERT_EQ(ulen,p->ulen());
  ASSERT_STREQ("23442 1234 23454 332345",p->cstr());
  ASSERT_TRUE(succ);
  list<asn_t>::iterator t = p->path.begin();
  ASSERT_EQ(t,p->sp_begin);
  delete p;
}

TEST(PathTest, PrependLoopReturnsFalse)
{
  std::string input = "['1234', '23454', '332345']";
  Path *p = new Path(input);

  bool succ = p->prepend("23454",false);
  ASSERT_FALSE(succ) ;
  ASSERT_TRUE(p->have_loop);
  delete p;

  p = new Path(input);
  p->prepend("22",false);
  p->prepend("24",false);
  succ = p->prepend("22",false);
  ASSERT_FALSE(succ);
  ASSERT_TRUE(p->have_loop);
  delete p;
}

TEST(PathTest, CopyConstructor)
{
  Path p1("['1', '2', '3', '4']");
  Path p2(p1);
  ASSERT_STREQ("1 2 3 4",p1.cstr());
  ASSERT_STREQ("1 2 3 4",p2.cstr()) << "Copied Path string doesn't match";
}

TEST(PathTest, CopyConstructorCopiesSurePathBegin)
{
  Path p1("['1', '2', '3', '4']");
  p1.prepend("99");
  Path p2(p1);
  ASSERT_NE(p2.path.end(),p2.sp_begin);
}
TEST(PathTest, IdenticalPathsCompareEqual)
{
  Path p1("['1', '2', '3', '4']");
  Path p2("['1', '2', '3', '4']");
  ASSERT_TRUE( (!(p1 < p2) && !(p2 < p1)) );
}

TEST(PathTest, Comparison)
{
  Path p1("['1', '2', '3', '4']");
  Path p2(p1);
  Path p3(p1);
  p2.prepend("9",false);
  p3.prepend("8",true);
  Path p1_dup(p1);

  p1_dup.incr_freq();
  ASSERT_LT(p1_dup, p1) << "Higher frequency paths should be lower";

  ASSERT_LT(p1, p2) << "Shorter paths should be lower";
  ASSERT_LT(p1, p3) << "Shorter paths should be lower";

  ASSERT_LT(p3, p2) << "Less uncertain paths should be lower";
}

TEST(PathTest, PrintEmptyPathIsOkay)
{
  Path p;

  ASSERT_STREQ("",p.cstr());
}

