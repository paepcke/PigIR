
--Avoid java.lang.OutOfMemoryError: Java heap space (execmode: -x local)
--Set sort heap space to 10MB:
--set io.sort.mb 50;

-- Load input from the file named Mary, and call the single 
-- field in the record 'line'.
poemLines = load 'bad/file/path' USING PigStorage() AS(line:chararray);
DUMP poemLines;