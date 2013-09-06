#include <gtest/gtest.h>
#include <unordered_set>
#include "../infer.h"
#include "../structures.h"

TEST(ASNConversion, NormalEncode)
{
  string asn1 = "12342";
  asn_t res = ASN_encode(asn1);
  ASSERT_EQ(12342,res);
}

TEST(ASNConversion, NormalDecode)
{
  asn_t asn = 12342;
  const char *res = ASN_decode(asn);
  ASSERT_STREQ("12342",res);
}

TEST(ASNConversion, FloatEncode)
{
  string asn1 = "3.123";
  asn_t res = ASN_encode(asn1);
  ASSERT_EQ(531230,res);
}

TEST(ASNConversion, FloatDecode)
{
  asn_t asn1 = 720123;
  const char *res = ASN_decode(asn1);
  ASSERT_STREQ("22.0123",res);
}
 
TEST(ASNConversion, ASNsEncodeUniquely)
{
  asn_t test;
  std::unordered_set<asn_t> seen;
  for (test = 1; test < 150000; test++) {
    std::pair<std::unordered_set<asn_t>::iterator, bool> result;
    result = seen.insert(test);
    ASSERT_EQ(true, result.second) << test << "encoded to a duplicate value";
  }
}

TEST(Flags, FlagSetWorksCorrectly) 
{
  FLAGS_INIT(flags);
  ASSERT_EQ(0,flags);

  FLAG_SET(flags, FLAG_DUMP_GRAPH);
  ASSERT_TRUE(FLAG_GET(flags,FLAG_DUMP_GRAPH))
    << "Flag wasn't set correctly. Flag: " 
    << std::bitset<32>(FLAG_DUMP_GRAPH) 
    << " Flags: " << std::bitset<32>(FLAG_GET(flags,FLAG_DUMP_GRAPH));

  FLAG_UNSET(flags, FLAG_DUMP_GRAPH);
  ASSERT_FALSE(FLAG_GET(flags,FLAG_DUMP_GRAPH))
    << "Flag wasn't unset correctly. Flag: " 
    << std::bitset<32>(FLAG_DUMP_GRAPH) 
    << " Flags: " << std::bitset<32>(FLAG_GET(flags,FLAG_DUMP_GRAPH));
}
