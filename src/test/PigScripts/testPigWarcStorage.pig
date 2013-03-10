/* 
      
*/       

-- STORE command for the word count:
--%declare WORD_COUNT_STORE_COMMAND "STORE sorted INTO '$WORD_COUNT_DEST' USING PigStorage(',');";

REGISTER $USER_CONTRIB/piggybank.jar;
REGISTER $PIGIR_HOME/target/pigir.jar;

docs = LOAD '$WARC_FILE'
		USING edu.stanford.pigir.warc.WarcLoader
       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray,
           optionalHeaderFlds:bytearray, content:chararray);

DUMP docs;

--STORE docs INTO '/tmp/test/testPigWarcStorageResult.warc' USING edu.stanford.pigir.warc.PigWarcStorage();
