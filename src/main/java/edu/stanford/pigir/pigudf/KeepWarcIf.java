package edu.stanford.pigir.pigudf;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import edu.stanford.pigir.warc.PigWarcRecord;
import edu.stanford.pigir.warc.WarcFilter;

/**
 * @author paepcke
 *
 *	Filter UDF for Pig. Takes a WARC tuple, a WARC header field name,
 *	and a regex. Returns true if the respective header field's value
 *	matches the regex, else returns false.
 */
public class KeepWarcIf extends FilterFunc {

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
     * 
     */
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        if (input.size() != 3) {
        	throw new IOException("Warc filters require three arguments: a WARC tuple, a WARC field identifier, and a regular expression.");
        }
        Tuple warcValuesTuple = null;
        String warcFldName    = null;
        String regex          = null;
        try {
            warcValuesTuple  = (Tuple) input.get(0);
            warcFldName      = (String) input.get(1); 
            regex            = (String) input.get(2); 
        } catch (ExecException ee) {
            throw new IOException("Caught exception processing input row ", ee);
        }
        PigWarcRecord warcRec = new PigWarcRecord(warcValuesTuple);
        WarcFilter warcFilter = new WarcFilter(regex, warcFldName);
        return warcFilter.matches(warcRec);
    }
}
