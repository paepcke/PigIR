/* 
   Given a WARC file or a directory of compressed
   or non-compressed WARC files, create two new archives:
   one containing a sample of the given archive, and another
   of the original archive with the samples removed.
   Write the result to a target directory, 

   Documents longer than 700000 are skipped.

   Start this Pig script via the stripHTML bash script, like this:
      stripHTML [options] <warcSourceFilePathOnHDFS>
   where options are:

	 [{-h | --help}]
	 [{-v | --version}]
	 [{-x | --execmode] {local | mapreduce}]
	 [{-d | --destdir} <destinationDirectory>] (default: pwd if execmode==local; 
	      		   			   	     else '/user/<username>')
   Environment assumptions:
      * $USER_CONTRIB  points to location of piggybank.jar and jsoup-1.5.2.jar
      * $PIGIR_HOME    points to location project root (above target dir)
      * $DEST_SAMPLE   destination of sample file
      * $DEST_MAIN     destination of the archive copy with sample removed
      * $WARC_FILE     archive to take sample from
      * $PERCENTAGE    sample size as percentage (may be real number or int)
*/       

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;
REGISTER $USER_CONTRIB/jsoup.jar;

docs = LOAD '$WARC_FILE'
       USING edu.stanford.pigir.warc.WarcLoader
       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray,
           optionalHeaderFlds:bytearray, content:bytearray);

docsLenFiltered = FILTER docs BY SIZE(content) < 700000;
theSample = SAMPLE docsLenFiltered $PERCENTAGE/100.0;

-- Create this tuples of structure (the Group: chararray being one warcRecordId):
-- co_group: {group: chararray,
--            docsLenFiltered: {(warcRecordId: chararray,contentLength: int,date: chararray,warc_type: chararray,optionalHeaderFlds: bytearray,content: bytearray)},              
--            theSample:       {(warcRecordId: chararray,contentLength: int,date: chararray,warc_type: chararray,optionalHeaderFlds: bytearray,content: bytearray)}
--           }

co_group = COGROUP docsLenFiltered BY warcRecordId, theSample BY warcRecordId;

-- Keep from original only tuples that were NOT selected into the sample:
origMinusSampleCoGroup = FILTER co_group BY IsEmpty(theSample);

origMinusSample = FOREACH origMinusSampleCoGroup GENERATE FLATTEN(docsLenFiltered);

DESCRIBE origMinusSample;
DESCRIBE theSample;

STORE origMinusSample INTO '$DEST_MAIN' USING edu.stanford.pigir.warc.PigWarcStorage();
STORE theSample INTO '$DEST_SAMPLE' USING edu.stanford.pigir.warc.PigWarcStorage();

