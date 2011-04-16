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
      * $PIG_HOME points to root of Pig installation
      * $USER_CONTRIB points to location of PigIR.jar
      * $USER_CONTRIB points to location of jsoup-1.5.2.jar

   $PIG_HOME and $USER_CONTRIB are assumed to be passed in
   via -param command line parameters. The pigrun script that
   is used by buildWebBaseIndex takes care of this. Additionally,
   the following env vars must be set:
   
       $CRAWL_SOURCE=crawl source, page numbers, start/stop sites as per examples above
   	   $TARGET_DIR=directory to hold the two index files. Defaults to
   	                   /user/paepcke/webbase/Index
   	   $WORD_INDEX_FILE=file name for index of words->doc/position. Defaults to
   	                   ${CRAWL_SOURCE}WordIndex
       $URL_INDEX_FILE=file name for index of docID->URL. Defaults to
       				   ${CRAWL_SOURCE}URLIndex
*/   

%default TARGET_DIR '/user/paepcke/webbase/Index';
%default WORD_INDEX_FILE '${CRAWL_SOURCE}WordIndex';
%default URL_INDEX_FILE '${CRAWL_SOURCE}URL';

REGISTER $PIG_HOME/contrib/piggybank/java/piggybank.jar;
REGISTER $USER_CONTRIB/PigIR.jar;
REGISTER $USER_CONTRIB/jsoup-1.5.2.jar

docs = LOAD '$CRAWL_SOURCE'
	USING pigir.webbase.WebBaseLoader()
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
						pigir.pigudf.IndexOneDoc(pigir.pigudf.GetLUID(), content) AS postingsPackage; 

-- Now we have:
--    indexPackages: {url:chararray,
--					  date: chararray,
--					  pageSize: int,
--					  postingsPackage: (postings: (token: chararray,docID: chararray,tokenPos: int))} 
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
--    (38700_0,6,http://access.usgs.gov/robots.txt)
--    (38700_1,777,http://access.usgs.gov/)

STORE URLMap INTO '$TARGET_DIR/$URL_INDEX_FILE'
			 USING PigStorage; 
--			 USING org.apache.pig.piggybank.storage.CSVExcelStorage;

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

oneColumnPostings = FOREACH oneColumnNestedPostings 
					GENERATE FLATTEN($0) 
					AS (token:chararray, docID:chararray, tokenPos:chararray);
					
-- Now finally: we have:(token,docID,tokenPos)  
-- 		         	    (token,docID,tokenPos)

-- Take out the summary tuples; the (docID,'na',numTokens) that
-- were in each row:
oneColumnPostingsOnly = FILTER oneColumnPostings BY docID neq 'na';

-- Order and save:

sortedPostings = ORDER oneColumnPostingsOnly BY token PARALLEL 5;

STORE sortedPostings INTO '$TARGET_DIR/$WORD_INDEX_FILE'
					 USING PigStorage; 
--					 USING org.apache.pig.piggybank.storage.CSVExcelStorage;
