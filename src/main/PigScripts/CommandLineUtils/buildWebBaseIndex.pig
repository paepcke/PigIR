/* 
   Given a WebBase crawl, and optionally a number of pages,
   start, and stop sites, extract the contained Web pages,
   remove their HTML tags, and remove stopwords. Then
   build an index consisting of two tables:
   
     <CrawlName>WordIndex:
     	 (word,docID,pos)
           
     <CrawlName>URLIndex:
         (docID,url)
         
   By default output is to HDFS directory /user/paepcke/webbase/Index

   This script via buildWebBaseIndex:
   
      buildWebBaseIndex <crawlName>[:numPages[:startSite[:endSite]]]
   
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
   is used by buildWebBaseIndex takes care of this. Additionally,
   the following env vars must be passed in via -param:
   
       $CRAWL_SOURCE=crawl source, page numbers, start/stop sites as per examples above
       $URL_MAP_DEST: Full destination path for storing the docID-->URL index.
	   		Example: /home/doe/gov-03-2007_urlMap.idx
       $INDEX_DEST  : Full destination path for the index.
	   		Example: /home/doe/gov-03-2007_index.idx
       $TMP_INDEX_DEST: Full path to a file into which the unsorted index is
                        temporarily stored.

   The buildWebBaseIndex script constructs these parameters from its
   command line parameters.
*/   

-- STORE commands for storing the URL->docID mapping:
%declare URL_MAP_STORE_COMMAND "STORE URLMap INTO '$URL_MAP_DEST' USING PigStorage(',');";

-- STORE and LOAD commands for the intermediate, temporary unsorted index:
%declare TMP_INDEX_STORE_COMMAND "STORE oneColumnPostings INTO '$TMP_INDEX_DEST' USING BinStorage;";
%declare TMP_INDEX_LOAD_COMMAND "LOAD '$TMP_INDEX_DEST' USING BinStorage AS (token:chararray,docID:chararray,tokenPos:int);";

-- STORE command for the final index:
%declare INDEX_STORE_COMMAND "STORE sortedIndex INTO '$INDEX_DEST' USING PigStorage(',');";

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar

docs = LOAD '$CRAWL_SOURCE'
	USING edu.stanford.pigir.webbase.WebBaseLoader()
	AS (url:chararray,
	    date:chararray,
	 	pageSize:int,
	 	position:int,
	 	docIDInCrawl:int,
	 	httpHeader,
	 	content:chararray);

indexPackages = FOREACH docs GENERATE 
			url, 
			date, 
			pageSize, 
			edu.stanford.pigir.pigudf.IndexOneDoc(edu.stanford.pigir.pigudf.GetLUID(), content) AS postingsPackage; 

-- Now we have:
--    indexPackages: {url:chararray,
--		      date: chararray,
--		      pageSize: int,
--		      postingsPackage: (postings: (token: chararray,docID: chararray,tokenPos: int))} 
--
-- Two example data rows, each corresponding to one Web page:
--    (http://access.usgs.gov/robots.txt,
--	   Mon Mar  3 13:55:50 2008,
--     239,
--     ((38400_0,na,6),(User,38400_0,0),(agent,38400_0,1),...(...)))
--    (http://access.usgs.gov/,
--     Mon Mar  3 13:56:03 2008,
--     5946,
--     ((38400_1,na,777),(HTML,38400_1,0),...(...)))
--
-- The first tuple in each postingsPackage has 'na' for the docID slot. This tuple summarizes
-- the rest of the token/docID/tokenPos triplets. Its content is docID/'na'/numTokens for
-- this row's document.  
--
-- We now first build an table that maps each internally used docID
-- to the document's URL:

URLMapPrep = FOREACH indexPackages GENERATE url, flatten(postingsPackage);

-- We now have this schema for each row:
-- URLMapPrep: {url: chararray,
--  	   	    postingsPackage::postings: (token: chararray,docID: chararray,tokenPos: int)}
-- Data example row:
--  (http://access.usgs.gov/robots.txt,
--  (38600_0,na,6),(User,38600_0,0),(agent,38600_0,1),(  ),...(  )))
--
URLMap = FOREACH URLMapPrep GENERATE 
			postings.docID, url;  

-- We now have schema URLMap: {docID: chararray, url: chararray}
-- Example for two documents:
--    (38700_0,http://access.usgs.gov/robots.txt)
--    (38700_1,http://access.usgs.gov/)

-- Store the docID-->URL index: 
$URL_MAP_STORE_COMMAND

-- Now prepare the index itself:
flatPostings = FOREACH indexPackages GENERATE flatten(postingsPackage);

-- Here we have schema:
--    flatPostings: {postingsPackage::postings: (token: chararray,docID: chararray,tokenPos: int)}
-- Data examples: 
--    ((39100_0,na,6),(User,39100_0,0),(agent,39100_0,1),(  ),...(  ))
--    ((39100_1,na,777),(HTML,39100_1,0),(HEAD,39100_1,1),(  ),... (   ),))

oneColumnNestedPostings = FOREACH flatPostings GENERATE FLATTEN(TOBAG(*));

-- Now we have ((token,docID,tokenPos))  
--             ((token,docID,tokenPos))

oneColumnPostings = FOREACH oneColumnNestedPostings GENERATE flatten($0);

-- Now finally: we have:(token: chararray,docID: chararray,tokenPos: int)
--                      (token: chararray,docID: chararray,tokenPos: int)
--                                 ...

-- The only way to get the index sorted is to write it out, and read
-- it back in. Everything I tried for making it work without this 
-- intermediate step failed with "Cannot cast String to DataTuple"...

$TMP_INDEX_STORE_COMMAND

theIndex = $TMP_INDEX_LOAD_COMMAND

sortedIndex = ORDER theIndex BY token PARALLEL 5;

$INDEX_STORE_COMMAND
