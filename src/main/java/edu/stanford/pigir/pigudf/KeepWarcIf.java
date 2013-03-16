package edu.stanford.pigir.pigudf;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.jsoup.select.Evaluator.Matches;

import edu.stanford.pigir.warc.PigWarcRecord;

/**
 * @author paepcke
 *
 *	Filter UDF for Pig. Takes a WARC tuple, a WARC header field name,
 *	and a regex. Returns true if the respective header field's value
 *	matches the regex, else returns false.
 *
 * Assuming that $WARC_FIELD holds the name of the WARC field
 * to be filtered on, that $REGEX contains the (Java) regular
 * expression, and that the WARC file was loaded in your Pig
 * script via:
 * 
 * docs = LOAD '$WARC_FILE'
 *        USING edu.stanford.pigir.warc.WarcLoader
 *        AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray,
 *            optionalHeaderFlds:bag{fldNameVal:tuple(fldName:chararray,fldVal:chararray)}, content:bytearray);
 *            
 * -- Create new tuples from these old ones that have a WARC field name, and a regular expression
 * -- appended:
 * 
 * extended = FOREACH docsLenFiltered GENERATE                                     
 *              warcRecordId,contentLength,date,warc_type,optionalHeaderFlds,content,'$WARC_FIELD','$REGEX';                                                      
 * 
 * -- Finally, do the filtering:                                                                              
 * keepers = FILTER extended BY edu.stanford.pigir.pigudf.KeepWarcIf(*);
 */

public class KeepWarcIf extends FilterFunc {

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
     * 
     */
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        if (input.size() != 8) {
        	throw new IOException("Warc filters require eight arguments: all six WARC tuple fields, a WARC field identifier, and a regular expression. Got: " + input.toString());
        }
        String recID             = null;
        int contentLength	     = -1;
        String date              = null;
        String warcType          = null;
        DefaultDataBag optionalWarcFlds     = null;
        DataByteArray content    = null;
        String warcFldNameToTest = null;
        String regex             = null;
        
        try {
            recID             = (String) input.get(0);
            contentLength     = (Integer)input.get(1);
            date			  = (String) input.get(2);
            warcType		  = (String) input.get(3);
            optionalWarcFlds  = (DefaultDataBag) input.get(4);
            content	   		  = (DataByteArray) input.get(5);
            warcFldNameToTest = (String) input.get(6); 
            regex             = (String) input.get(7); 
        } catch (ExecException ee) {
            throw new IOException("Caught exception processing input row ", ee);
        }
        
        Pattern regexPattern = Pattern.compile(regex);
        Matcher m = null;
        boolean foundFld = false;
        
        if (warcFldNameToTest.compareToIgnoreCase(PigWarcRecord.WARC_RECORD_ID) == 0) {
        	m = regexPattern.matcher(recID);
        	foundFld = true;
        }
        else if (warcFldNameToTest.compareToIgnoreCase(PigWarcRecord.CONTENT_LENGTH) == 0) {
        	m = regexPattern.matcher(Integer.toString(contentLength));
        	foundFld = true;
        }
        else if (warcFldNameToTest.compareToIgnoreCase(PigWarcRecord.WARC_DATE) == 0) {
        	m = regexPattern.matcher(date);
        	foundFld = true;
        }
        else if (warcFldNameToTest.compareToIgnoreCase(PigWarcRecord.WARC_TYPE) == 0) {
        	m = regexPattern.matcher(warcType);
        	foundFld = true;
        }
        else if (warcFldNameToTest.compareToIgnoreCase(PigWarcRecord.CONTENT) == 0) {
        	//******************
        	String contentStr = new String(content.get());
        	//******************
        	m = regexPattern.matcher(new String(content.get()));
        	//******************
        	boolean doesMatch = m.matches();
        	//******************
        	foundFld = true;
        }
        else {
        	// Header field to match is either part of the catch-all optionalWarcFlds bag of two-tuples,
        	// or it does not exist:
        	@SuppressWarnings("unchecked")
			Iterator<Tuple> fldIt = (Iterator<Tuple>) optionalWarcFlds.iterator();
        	while (fldIt.hasNext()) {
        		Tuple fldNameValPair = fldIt.next();
        		String fldName = (String)fldNameValPair.get(0);
        		String fldVal  = (String)fldNameValPair.get(1);
        		if (fldName.compareToIgnoreCase(warcFldNameToTest) == 0) {
        			m = regexPattern.matcher(fldVal);
        			foundFld = true;
        			break;
        		}
        	}
        	if (! foundFld)
        		throw new IOException("Attempt to match non-existent WARC field name '" + warcFldNameToTest + "' against regex '" + regex + "'");
        }
        return (m.matches());
    }
}
