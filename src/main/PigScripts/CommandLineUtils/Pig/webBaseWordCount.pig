/* 
#resultAlias sorted -- DO NOT MOVE OR DELETE

   Given a WebBase crawl, and optionally a number of pages,
   start, and stop sites, extract the contained Web pages,
   remove their HTML tags, and remove stopwords. Then
   count all words, and store them on HDFS in alpha order
   as:
          (word, count)
         
   By default output is to HDFS directory /user/paepcke/webbase/WordCount

   You should run this script via webBaseWordCount:
   
      webBaseWordCount <crawlName>[:numPages[:startSite[:endSite]]]
   
   Examples:
   
    All pages from crawl gov-03-2009:
      buildWebBaseIndex gov-03-2009
      
    The first 11 pages of crawl gov-03-2009:
      buildWebBaseIndex gov-03-2009:11
      
    The first 11 pages of crawl gov-03-2009, starting at site access.usgs.gov
      buildWebBaseIndex gov-03-2009:11:access.usgs.gov

    The first 11 pages of crawl gov-03-2009, starting at site access.usgs.gov,
    and ending at acrim.jpl.nasa.gov
      buildWebBaseIndex gov-03-2009:11:access.usgs.gov:acrim.jpl.nasa.gov
      
    All pages of crawl gov-03-2009, starting at site access.usgs.gov
      buildWebBaseIndex gov-03-2009::access.usgs.gov
    
    Environment expectations:
      * $USER_CONTRIB points to location of piggybank.jar
      * $USER_CONTRIB points to location of jsoup-1.5.2.jar
      * $PIGIR_HOME   points to location project root (above target dir)

   $USER_CONTRIB are assumed to be passed in
   via -param command line parameters. The pigrun script that
   is used by webBaseWordCount takes care of this. Additionally,
   the following env vars must be passed in via -param:
   
       $CRAWL_SOURCE=crawl source, page numbers, start/stop sites as per examples above
          $WORD_COUNT_DEST  : Full destination path for the word count.
	   		Example: /home/doe/gov-03-2007_wordCount.cnt

   The webBaseWordCount script constructs these parameters from its
   command line parameters.
*/   

-- STORE command for the word count:
%declare WORD_COUNT_STORE_COMMAND "STORE sorted INTO '$WORD_COUNT_DEST' USING PigStorage(',');";

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $PIGIR_HOME/target/classes/jsoup.jar

docs = LOAD '$CRAWL_SOURCE'
	USING edu.stanford.pigir.webbase.WebBaseLoader()
	AS (url:chararray,
	    date:chararray,
	 	pageSize:int,
	 	position:int,
	 	docIDInCrawl:int,
	 	httpHeader,
	 	content:chararray);

strippedDocs = FOREACH docs GENERATE edu.stanford.pigir.pigudf.StripHTML(content);

/* Tokenize, using default regexp for splitting (the null), eliminiating
   stopwords (first '1' in parameter list), and preserving URLs 
   (second '1' in parms):
*/   
strippedWords = FOREACH strippedDocs GENERATE FLATTEN(edu.stanford.pigir.pigudf.RegexpTokenize(content, null, 1, 1));

strippedGroupedWords = GROUP strippedWords BY $0;

-- Get (word, count) pairs:
wordCounts = FOREACH strippedGroupedWords GENERATE $0 AS word:chararray,COUNT($1) AS count:long;
sorted     = ORDER wordCounts BY word PARALLEL 5; 

$WORD_COUNT_STORE_COMMAND;

