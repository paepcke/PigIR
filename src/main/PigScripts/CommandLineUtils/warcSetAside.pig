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
      * $DEST          destination WARC name (directory if source is a directory, else dest file name).
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

DUMP theSample;



/*
strippedWarc = FOREACH docsLenFiltered GENERATE 
	     warcRecordId,contentLength,date,warc_type,
	     optionalHeaderFlds, edu.stanford.pigir.pigudf.StripHTML(content);

STORE strippedWarc INTO '$STRIPPED_DEST' USING edu.stanford.pigir.warc.PigWarcStorage();
*/
