REGISTER $PIG_HOME/piggybank/java/piggybank.jar;
REGISTER contrib/PigIRWithSource.jar;
			
docs = LOAD '2003-06:10'
			USING pigir.webbase.WebBaseLoader()
			AS (url:chararray, date:chararray, pageSize:int, position:int, docidInCrawl:int, httpHeader:chararray, content:chararray);
					
DUMP docs;
DESCRIBE docs;

