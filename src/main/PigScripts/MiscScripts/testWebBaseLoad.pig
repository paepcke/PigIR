REGISTER $PIG_HOME/piggybank/java/piggybank.jar;
REGISTER contrib/PigIRWithSource.jar;
			
docs = LOAD '2003-06:2:www.hp.com:www.ssa.gov'
			USING pigir.webbase.WebBaseLoader()
			AS (url:chararray, date:chararray, pageSize:int, position:int, docidInCrawl:int, httpHeader:chararray, content:chararray);
					
DUMP docs;
DESCRIBE docs;


-- docs = LOAD 'crawled_hosts.1105.tx.f:1:www.hp.com'
-- docs = LOAD 'crawled_hosts.1105.tx.f:2:www.hp.com:www.ssa.gov'
