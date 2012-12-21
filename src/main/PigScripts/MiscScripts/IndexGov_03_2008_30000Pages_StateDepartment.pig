REGISTER $PIG_HOME/contrib/piggybank/java/piggybank.jar;
REGISTER contrib/PigIRWithSource.jar;

docs = LOAD 'gov-03-2008:30000:www.state.gov:www.state.gov'
	   USING pigir.webbase.WebBaseLoader()
	   AS (
	   		url:chararray,
			date:chararray,
			pageSize:int,
			position:int,
			docidInCrawl:int,
			httpHeader:chararray,
			content:chararray);
		
rawIndex = FOREACH docs GENERATE 
		    pigir.pigudf.IndexOneDoc(pigir.pigudf.GetLUID(), content);
		    
flatRawIndex = FOREACH rawIndex GENERATE flatten($0);
index = ORDER flatRawIndex BY token ASC, docID ASC PARALLEL 5; 
STORE index INTO 'Datasets/gov-03-2008-wwwStateGov-30000Pages-index.csv'
			USING PigStorage(',');
