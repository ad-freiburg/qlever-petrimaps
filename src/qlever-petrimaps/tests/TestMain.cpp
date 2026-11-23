// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <string>

#include "qlever-petrimaps/server/Requestor.h"
#include "util/Misc.h"
#include "util/Test.h"

using petrimaps::Requestor;

// _____________________________________________________________________________
int main(int argc, char** argv) {
  UNUSED(argc);
  UNUSED(argv);

  {
    // example from #58
    std::string q = R"(PREFIX geo: <http://www.opengis.net/ont/geosparql#>
WITH %second as {
?rad wdt:P31 wd:Q5 . }
With %first AS {
{ # inner }
  SELECT ?rad WHERE { INCLUDE %second } } .
?random wdt:P31 wd:Q901 . }
SELECT DISTINCT ?pers ?geo {
  { SELECT ?random WHERE { INCLUDE %first } } # comment
  ?pers wdt:P108 ?org . FILTER(?x < 3 && ?y > 2) FILTER(?s != "}select{")
  }
LIMIT 10 # trailing { comment)";

    auto r = Requestor::getMainQuery(q);
    TEST(r.first, ==, q.find("SELECT DISTINCT"));
    TEST(r.second, ==, q.find("LIMIT 10") + 8);
  }

  {
    // example from #58, even worse with a SELECT in a string
    std::string q = R"(PREFIX geo: <http://www.opengis.net/ont/geosparql#>
WITH %second as {
?rad wdt:P31 wd:Q5 . }
With %first AS {
{ SELECT * WHERE { ?a ?b "SELECT * WHERE {" } }
{  SELECT ?rad WHERE { INCLUDE %second } } .
?random wdt:P31 wd:Q901 . }
SELECT DISTINCT ?pers ?geo {
  { SELECT ?random WHERE { INCLUDE %first } } # comment
  ?pers wdt:P108 ?org . FILTER(?x < 3 && ?y > 2) FILTER(?s != "}select{")
  }
LIMIT 10 # trailing { comment)";

    auto r = Requestor::getMainQuery(q);
    TEST(r.first, ==, q.find("SELECT DISTINCT"));
    TEST(r.second, ==, q.find("LIMIT 10") + 8);
  }

  {
    // example from #58, even worser with a " inside a ' string
    std::string q = R"(PREFIX geo: <http://www.opengis.net/ont/geosparql#>
WITH %second as {
?rad wdt:P31 wd:Q5 . }
With %first AS {
{ SELECT * WHERE { ?a ?b 'SELECT * WHERE boese"{' } }
{  SELECT ?rad WHERE { INCLUDE %second } } .
?random wdt:P31 wd:Q901 . }
SELECT DISTINCT ?pers ?geo {
  { SELECT ?random WHERE { INCLUDE %first } } # comment
  ?pers wdt:P108 ?org . FILTER(?x < 3 && ?y > 2) FILTER(?s != "}select{")
  }
LIMIT 10 # trailing { comment)";

    auto r = Requestor::getMainQuery(q);
    TEST(r.first, ==, q.find("SELECT DISTINCT"));
    TEST(r.second, ==, q.find("LIMIT 10") + 8);
  }

  {
    // example from #58, even worser with a " inside a """ string
    std::string q = R"(PREFIX geo: <http://www.opengis.net/ont/geosparql#>
WITH %second as {
?rad wdt:P31 wd:Q5 . }
With %first AS {
{ SELECT * WHERE { ?a ?b """SELECT * WHERE boese"{""" } }
{  SELECT ?rad WHERE { INCLUDE %second } } .
?random wdt:P31 wd:Q901 . }
SELECT DISTINCT ?pers ?geo {
  { SELECT ?random WHERE { INCLUDE %first } } # comment
  ?pers wdt:P108 ?org . FILTER(?x < 3 && ?y > 2) FILTER(?s != "}select{")
  }
LIMIT 10 # trailing { comment)";

    auto r = Requestor::getMainQuery(q);
    TEST(r.first, ==, q.find("SELECT DISTINCT"));
    TEST(r.second, ==, q.find("LIMIT 10") + 8);
  }

  {
    // example from #58, even worser with a ' inside a ''' string
    std::string q = R"(PREFIX geo: <http://www.opengis.net/ont/geosparql#>
WITH %second as {
?rad wdt:P31 wd:Q5 . }
With %first AS {
{ SELECT * WHERE { ?a ?b '''SELECT * WHERE boese'"{''' } }
{  SELECT ?rad WHERE { INCLUDE %second } } .
?random wdt:P31 wd:Q901 . }
SELECT DISTINCT ?pers ?geo {
  { SELECT ?random WHERE { INCLUDE %first } } # comment
  ?pers wdt:P108 ?org . FILTER(?x < 3 && ?y > 2) FILTER(?s != "}select{")
  }
LIMIT 10 # trailing { comment)";

    auto r = Requestor::getMainQuery(q);
    TEST(r.first, ==, q.find("SELECT DISTINCT"));
    TEST(r.second, ==, q.find("LIMIT 10") + 8);
  }

  {
    std::string q = "SELECT ?a WHERE { ?a ?b ?c }";

    auto r = Requestor::getMainQuery(q);
    TEST(r.first, ==, 0);
    TEST(r.second, ==, q.size());
  }

  {
    std::string q = "SELECT?a WHERE { ?a ?b ?c }";

    auto r = Requestor::getMainQuery(q);
    TEST(r.first, ==, 0);
    TEST(r.second, ==, q.size());
  }

  {
    std::string q = "SELECT?a WHERE {?a?b?c}";

    auto r = Requestor::getMainQuery(q);
    TEST(r.first, ==, 0);
    TEST(r.second, ==, q.size());
  }

  {
    std::string q =
        "# select\nPREFIX ex: <http://ex#>\nselect ?selection WHERE { "
        "?selection ex:select ?c }\n";

    auto r = Requestor::getMainQuery(q);
    TEST(r.first, ==, q.find("select ?selection"));
    TEST(r.second, ==, q.size() - 1);
  }

  {
    std::string q =
        "# select\nPREFIX ex: <http://ex#>\nselect?selection WHERE { "
        "?selection ex:select ?c }\n";

    auto r = Requestor::getMainQuery(q);
    TEST(r.first, ==, q.find("select?selection"));
    TEST(r.second, ==, q.size() - 1);
  }

  {
    std::string q =
        "# select\nPREFIX ex: <http://ex#>\nselect?selection WHERE { "
        "?selection?b?c }\n";

    auto r = Requestor::getMainQuery(q);
    TEST(r.first, ==, q.find("select?selection"));
    TEST(r.second, ==, q.size() - 1);
  }

  {
    // "ask" queries
    std::string q = "ASK { ?a ?b ?c }";

    auto r = Requestor::getMainQuery(q);
    TEST(r.first, ==, std::string::npos);
    TEST(r.second, ==, std::string::npos);
  }

  {
    // "ask" queries
    std::string q = "ASK{?a?b?c}";

    auto r = Requestor::getMainQuery(q);
    TEST(r.first, ==, std::string::npos);
    TEST(r.second, ==, std::string::npos);
  }
}
