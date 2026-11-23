// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <string>

#include "qlever-petrimaps/Misc.h"
#include "util/Misc.h"
#include "util/Test.h"

using petrimaps::getMainQuery;

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

    auto r = getMainQuery(q);
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

    auto r = getMainQuery(q);
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

    auto r = getMainQuery(q);
    TEST(r.first, ==, q.find("SELECT DISTINCT"));
    TEST(r.second, ==, q.find("LIMIT 10") + 8);
  }

  {
    // example from #58, even worser with a " inside a """ string
    std::string q = R"(PREFIX geo: <http://www.opengis.net/ont/geosparql#>
WITH %second as {
?rad wdt:P31 wd:Q5 . }
With %first AS {
{ SELECT * WHERE { $a ?b """SELECT * WHERE boese"{""" } }
{  SELECT $rad WHERE { INCLUDE %second } } .
?random wdt:P31 wd:Q901 . }
SELECT DISTINCT ?pers $geo {
  { SELECT ?random WHERE { INCLUDE %first } } # comment
  ?pers wdt:P108 ?org . FILTER(?x < 3 && ?y > 2) FILTER(?s != "}select{")
  }
LIMIT 10 # trailing { comment)";

    auto r = getMainQuery(q);
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

    auto r = getMainQuery(q);
    TEST(r.first, ==, q.find("SELECT DISTINCT"));
    TEST(r.second, ==, q.find("LIMIT 10") + 8);
  }

  {
    std::string q = "SELECT ?a WHERE { ?a ?b ?c }";

    auto r = getMainQuery(q);
    TEST(r.first, ==, 0);
    TEST(r.second, ==, q.size());
  }

  {
    std::string q = "SELECT?a WHERE { ?a ?b ?c }";

    auto r = getMainQuery(q);
    TEST(r.first, ==, 0);
    TEST(r.second, ==, q.size());
  }

  {
    std::string q = "SELECT?a WHERE {?a?b?c}";

    auto r = getMainQuery(q);
    TEST(r.first, ==, 0);
    TEST(r.second, ==, q.size());
  }

  {
    std::string q =
        "# select\nPREFIX ex: <http://ex#>\nselect ?selection WHERE { "
        "?selection ex:select ?c }\n";

    auto r = getMainQuery(q);
    TEST(r.first, ==, q.find("select ?selection"));
    TEST(r.second, ==, q.size() - 1);
  }

  {
    std::string q =
        "# select\nPREFIX ex: <http://ex#>\nselect?selection WHERE { "
        "?selection ex:select ?c }\n";

    auto r = getMainQuery(q);
    TEST(r.first, ==, q.find("select?selection"));
    TEST(r.second, ==, q.size() - 1);
  }

  {
    std::string q =
        "# select\nPREFIX ex: <http://ex#>\nselect?selection WHERE { "
        "?selection?b?c }\n";

    auto r = getMainQuery(q);
    TEST(r.first, ==, q.find("select?selection"));
    TEST(r.second, ==, q.size() - 1);
  }

  {
    // prefixes named select
    std::string q =
        "# select\nPREFIX select: <http://ex#>\nPREFIX select-x: <http://ex#>\n"
        "PREFIX x.select: <http://ex#>\nPREFIX \xC3\xA4select: <http://ex#>\n"
        "select ?selection WHERE { ?selection select:select ?c }\n";

    auto r = getMainQuery(q);
    TEST(r.first, ==, q.find("select ?selection"));
    TEST(r.second, ==, q.size() - 1);
  }

  {
    std::string q =
        "# select\nPREFIX ex: <http://ex#>\nselect ?selection WHERE { "
        "?selection ex:a\\#b ex:it\\'s }\n";

    auto r = getMainQuery(q);
    TEST(r.first, ==, q.find("select ?selection"));
    TEST(r.second, ==, q.size() - 1);
  }

  {
    std::string q =
        "# select\nPREFIX ex: <http://ex#>\nWITH %x AS { ?a ex:a\\#b ?c }\n"
        "WITH %y AS { ?a ex:it\\'s ?c }\n"
        "select ?selection WHERE { INCLUDE %x INCLUDE %y }\n";

    auto r = getMainQuery(q);
    TEST(r.first, ==, q.find("select ?selection"));
    TEST(r.second, ==, q.size() - 1);
  }

  {
    std::string q =
        "# select\nPREFIX ex: <http://ex#>\nWITH %x AS { ?a ex:p ?c "
        "FILTER(?c<\"a>b\") }\nselect ?selection WHERE { ?selection ex:select "
        "?c FILTER(?c<\"}>{\"&&?c>?d) }\n";

    auto r = getMainQuery(q);
    TEST(r.first, ==, q.find("select ?selection"));
    TEST(r.second, ==, q.size() - 1);
  }

  {
    std::string q =
        "# select\rPREFIX ex: <http://ex#>\rselect ?selection WHERE { "
        "?selection ex:select ?c } # comment\r";

    auto r = getMainQuery(q);
    TEST(r.first, ==, q.find("select ?selection"));
    TEST(r.second, ==, q.find(" # comment"));
  }

  {
    std::string q =
        "# select\nPREFIX ex: <http://ex#>\nselect ?selection WHERE { "
        "?selection ex:select \"\"\"a\"}\"\"\"@en, '''b'}'''^^ex:t }\n";

    auto r = getMainQuery(q);
    TEST(r.first, ==, q.find("select ?selection"));
    TEST(r.second, ==, q.size() - 1);
  }

  {
    std::string q =
        "SELECT ?a WHERE { ?a ?b ?c } # select { after where\n"
        "ORDER BY DESC(?a) # order } select\n"
        "# un \"commentaire\" <x>\n  \n";

    auto r = getMainQuery(q);
    TEST(r.first, ==, 0);
    TEST(r.second, ==, q.find("DESC(?a)") + 8);
  }

  {
    // "ask" queries
    std::string q = "ASK { ?a ?b ?c }";

    auto r = getMainQuery(q);
    TEST(r.first, ==, std::string::npos);
    TEST(r.second, ==, std::string::npos);
  }

  {
    // "ask" queries
    std::string q = "ASK{?a?b?c}";

    auto r = getMainQuery(q);
    TEST(r.first, ==, std::string::npos);
    TEST(r.second, ==, std::string::npos);
  }
}
