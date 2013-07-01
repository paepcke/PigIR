-- Input WARC file, and pull out content from .txt and .html records only

-- Invoke with: pig -param infile=<pathRelToUserDir> -param outfile=<outfileRelToUserDir> warcGetTextOnly.pig

REGISTER /dfs/rulk/0/paepcke/EclipseWorkspaces/PigIR/target/pigir.jar;

warcRecs = LOAD '$infile' USING edu.stanford.pigir.warc.WarcLoader AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray, optionalHeaderFlds:bag{optHeaders:(fldName:chararray, fldVal:chararray)}, content:bytearray);

fldsAndContent = FOREACH warcRecs GENERATE FLATTEN(optionalHeaderFlds), content;

goodValsAndContent = FOREACH fldsAndContent GENERATE REGEX_EXTRACT(optionalHeaderFlds::fldVal, '(.*\\.txt|.*\\.html)', 1), content;
goodValsFiltered = FILTER goodValsAndContent BY $0 IS NOT NULL;
goodVals = FOREACH goodValsFiltered GENERATE $1;

STORE goodVals INTO '$outfile' USING PigStorage();
