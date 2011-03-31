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
	 docidInCrawl:int,
	 httpHeader,
	 content:chararray);

/*
rawFusedIndices = FOREACH docs {
  			         wordIndexTuple = pigir.pigudf.IndexOneDoc(pigir.pigudf.GetLUID(), content);
					 --res = pigir.pigudf.First(wordIndexTuple);
					 --GENERATE url, wordIndexTuple.postingsInDoc;
					 --GENERATE url, wordIndexTuple.bagDocID.theBagDocID;
					 DESCRIBE wordIndexTuple;
					 GENERATE url;
   		   	      };	
*/

wordIndexTuple = FOREACH docs GENERATE pigir.pigudf.IndexOneDoc(pigir.pigudf.GetLUID(), content); 
DESCRIBE wordIndexTuple;
--docIDs = FOREACH wordIndexTuple GENERATE flatten(wordIndexTuple);
--DUMP docIDs;
--flatWordIndexTuple = FOREACH wordIndexTuple GENERATE flatten($0.$1); --(20900_1)all single-tuple w/ docID
--flatWordIndexTuple = FOREACH wordIndexTuple GENERATE flatten($0);    --cannot cast bag-->tuple      
--flatWordIndexTuple = FOREACH wordIndexTuple GENERATE flatten($0.$0); --cannot cast bag-->tuple      
--flatWordIndexTuple = FOREACH wordIndexTuple GENERATE $0; --cannot cast bag-->tuple
--flatWordIndexTuple = flatten(wordIndexTuple);  -- illegal
--flatWordIndexTuple = FOREACH wordIndexTuple GENERATE bagDocID; -- invalid alias
--flatWordIndexTuple = FOREACH wordIndexTuple GENERATE flatten(wordIndexTuple); --scalars can only 
 		     	     		    	     			        --be used with projection 
flatWordIndexTuple = FOREACH wordIndexTuple GENERATE flatten(wordIndexTuple.$0);
   --Caused by: java.lang.IllegalArgumentException: Pathname /gov-03-2008:2 from /gov-03-2008:2 
   --is not a valid DFS filename.
   --     at org.apache.hadoop.hdfs.DistributedFileSystem.getPathName(DistributedFileSystem.java:158)
   --     at org.apache.hadoop.hdfs.DistributedFileSystem.getFileStatus(DistributedFileSystem.java:453)
   --     at org.apache.hadoop.fs.FileSystem.exists(FileSystem.java:648)

DUMP flatWordIndexTuple;


--DUMP wordIndexTuple;


--flatRawIndex = FOREACH rawIndex GENERATE flatten($0);
			
--index = ORDER flatRawIndex BY $0;

--DUMP rawFusedIndices;
