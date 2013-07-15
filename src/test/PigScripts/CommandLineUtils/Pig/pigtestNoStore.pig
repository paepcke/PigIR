
--Avoid java.lang.OutOfMemoryError: Java heap space (execmode: -x local)
--Set sort heap space to 10MB:
--set io.sort.mb 50;

-- Load input from the file named Mary, and call the single 
-- field in the record 'line'.
poemLines = load 'src/test/resources/mary.txt' USING PigStorage() AS(line:chararray);

-- TOKENIZE splits the line into a field for each word.
-- flatten will take the collection of records returned by
-- TOKENIZE and produce a separate record for each one, calling the single
-- field in the record word.
words = foreach poemLines generate flatten(TOKENIZE(line)) as word;

-- Now group them together by each word.
grpd  = group words by word;

-- Count them
theCount  = foreach grpd generate group, COUNT(words);
-- Print out the results
-- DUMP theCount;
