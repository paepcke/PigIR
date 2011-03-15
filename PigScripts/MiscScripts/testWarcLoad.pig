REGISTER contrib/piggybank/java/piggybank.jar;
REGISTER contrib/PigIRWithSource.jar;
			
docs = LOAD 'Datasets/ClueWeb09_English_Sample.warc'
					USING pigir.WarcLoader
					AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray,
					    optionalHeaderFlds:bytearray, content:chararray);
DUMP docs;
DESCRIBE docs;
