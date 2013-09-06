#include <gtest/gtest.h>
#include "../structures.h"

class PathSetTest : public ::testing::Test 
{
  protected:
    virtual void SetUp() {
      p1_ = new Path("['1', '2', '3', '4']");
      p2_ = new Path("['5', '6', '7', '8']");
      origin = ASN_encode("1234");
      ps_.add(origin,p1_);
    }

    virtual void TearDown() {
      asn_t origin2 = ASN_encode("5678");
      ps_.clear(origin);
      ps_.clear(origin2);
    }

    Path *p1_;
    Path *p2_;
    PathSet ps_;
    asn_t origin;

};

TEST_F(PathSetTest,EmptyNew)
{
  PathSet ps;
  ASSERT_EQ(1,ps_.size(origin));
}

TEST_F(PathSetTest, AddInsertsNewPath)
{
  ps_.add(origin, p2_);
  ASSERT_EQ(2, ps_.size(origin)) << "Inserting path should increase size";
}

TEST_F(PathSetTest, DifferentPtrsCompareEqualIfPathsEqual)
{
  Path *p1dup = new Path(*p1_);
  PathPtrCmp cmp;

  EXPECT_NE(p1dup,p1_);
  ASSERT_FALSE(cmp(p1dup,p1_));
  ASSERT_FALSE(cmp(p1_, p1dup));
}

TEST_F(PathSetTest, ComparatorCorrectlyOrders)
{
  PathPtrCmp cmp;
  ASSERT_TRUE(cmp(p1_,p2_))
    << p1_->cstr(true) << " should come before " << p2_->cstr(true);
  ASSERT_FALSE(cmp(p2_,p1_))
    << p1_->cstr(true) << " should come before " << p2_->cstr(true);

  p1_->prepend("99",true);
  ASSERT_FALSE(cmp(p1_,p2_))
    << p2_->cstr(true) << " should come before " << p1_->cstr(true);
  ASSERT_TRUE(cmp(p2_,p1_)); 
  p2_->prepend("20",false);
  ASSERT_TRUE(cmp(p1_,p2_))
    << p1_->cstr(true) << " should come before " << p2_->cstr(true);
  ASSERT_FALSE(cmp(p2_,p1_))
    << p1_->cstr(true) << " should come before " << p2_->cstr(true);
}

TEST_F(PathSetTest, PeekNoCopyReturnsSameObject)
{
  Path *p = ps_.peek(origin, false);
  ASSERT_EQ(p1_, p) 
      << "Inserted object pointer not equal to return value of peek()";
}

TEST_F(PathSetTest, PeekCopyCreatesNew)
{
  Path *p = ps_.peek(origin, true);
  ASSERT_NE(p1_, p) 
      << "Inserted object pointer equal to return value of peek()";
}

TEST_F(PathSetTest, ClearRemovesElementsForOrigin)
{
  ASSERT_EQ(1,ps_.size(origin));
  ps_.clear(origin);
  ASSERT_EQ(0,ps_.size(origin));
  Path *p = ps_.peek(origin,false);
  ASSERT_EQ(0,p);
}

TEST_F(PathSetTest, AddIdenticalIncrFrequency) 
{
  ps_.add(origin,p1_);
  Path *ret = ps_.peek(origin,false);
  ASSERT_EQ(2, ret->frequency) 
    << "Adding duplicate "<< ret->cstr() << " should increment frequency";

  Path *p1dup = new Path(*p1_);
  ps_.add(origin,p1dup);
  ret = ps_.peek(origin,false);
  ASSERT_EQ(3, ret->frequency)
    << "Adding identical path (with different pointer) should"
    << "increment frequency.";
}

TEST_F(PathSetTest, PeekReturnsBestPath)
{
  Path *ret = ps_.peek(origin,false);
  ASSERT_EQ(ret, p1_);
  asn_t origin2 = ASN_encode("5678");
  ASSERT_EQ(0, ps_.size(origin2));

  Path *p4 = new Path(*p1_);
  p4->prepend("99",false);

  ps_.add(origin2,p4);
  ret = ps_.peek(origin2,false);
  ASSERT_EQ(p4, ret) 
    << "[" << p4 << "] " << p4->cstr(false) 
    << " should be better than " 
    << "[" << ret << "] " << ret->cstr(false);

  ps_.add(origin2,p2_);
  ret = ps_.peek(origin2,false);
  ASSERT_EQ(p2_, ret) 
    << "[" << p2_ << "] " << p2_->cstr(false) 
    << " should be better than " 
    << "[" << ret << "] " << ret->cstr(false);

  Path *p3 = new Path("1 2 3");
  ps_.add(origin2,p3);
  ret = ps_.peek(origin2,false);
  ASSERT_EQ(p3, ret) 
    << "[" << p3 << "] " << p3->cstr(false) 
    << " should be better than " 
    << "[" << ret << "] " << ret->cstr(false);

  ps_.clear(origin2);
}

