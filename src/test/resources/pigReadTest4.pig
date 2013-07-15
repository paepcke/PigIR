lines = LOAD 'foo.txt' 
      USING PigStorage() AS(line:chararray); -- Comment
