/* 
#resultAlias recCount  -- DO NOT MOVE OR DELETE

   Given a WARC file/dir, count the number of records in the file/dir.
   Output is via DUMP(), i.e. to stdout.

   It is convenient to start this script via warcRecordCount,
   or---less conveniently---via pigrun. warcRecordCount calls
   pigrun.

   Running via warcWordCount:
   
      warcRecordCount <srcFileOrDir>
   
   Environment assumptions (all taken care of by pigrun, if 
   you initialized it):
    
      * $USER_CONTRIB points to location of piggybank.jar
      * $PIGIR_HOME   points to location project root (above target dir)
      
   $USER_CONTRIB is assumed to be passed in
   via -param command line parameters. The pigrun script that
   is used by warcWordCount takes care of this.
      
*/       

-- STORE command for the word count:
%declare WORD_COUNT_STORE_COMMAND "DUMP recCount;";

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;

docs = LOAD '$WARC_FILE'
		USING edu.stanford.pigir.warc.WarcLoader
       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray,
           optionalHeaderFlds:bytearray, content:bytearray);


docsGroup = GROUP docs ALL;
recCount = FOREACH docsGroup GENERATE COUNT(docs);

$WORD_COUNT_STORE_COMMAND;

