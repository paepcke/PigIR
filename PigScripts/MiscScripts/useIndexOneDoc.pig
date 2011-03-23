REGISTER $PIG_HOME/contrib/piggybank/java/piggybank.jar;
REGISTER contrib/PigIRWithSource.jar;
			
docs = LOAD '2008-03:2'
			USING pigir.webbase.WebBaseLoader()
			AS (url:chararray, 
				date:chararray, 
				pageSize:int, 
				position:int, 
				docidInCrawl:int, 
				httpHeader:chararray, 
				content:chararray);
					
DUMP docs;
DESCRIBE docs;

