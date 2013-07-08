package edu.stanford.pigir.pigudf;

import java.io.IOException;
import java.security.InvalidParameterException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * @author paepcke
 * 
 * Pig UDF to 'fuse' multiple (String) columns of a tuple into 
 * a single String. Callers may specify the desired slice, using
 * a Pythonic array slice syntax. Callers may also provide a 
 * concatenation separator. Syntax:
 * 
 *      ConcatColumns(<sliceSpecStr>, {separatorStr | null}, <tuple>)
 *      
 * Examples:
 *    t = ("foo", "bar", 'fum')
 *    ConcatColumns('1:2', '', t)  ==> "bar"
 *    ConcatColumns('0:2', '', t)  ==> "foobar"
 *    ConcatColumns(':2', '', t)   ==> "foobar"
 *    ConcatColumns('1:', '', t)   ==> "barfum"
 *    ConcatColumns('0:-1', '', t)   ==> "foobar"
 *    ConcatColumns('2:2', '', t)   ==> "fum"
 *
 *	  ConcatColumns('0:-1', '|', t)   ==> "foo|bar"
 */
public class ConcatColumns  extends EvalFunc<String> {
	
	public static final int SLICE_SPEC_POS = 0;
	public static final int CONCAT_SEPARATOR_POS = 1;
	public static final int TUPLE_TO_FUSE_POS = 2;
	
	/* (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 * Expected input tuple: (sliceDefStr, {concatSeparatorStr | null}, (tuple-to-fuse-in))
	 * ConcatSeparator of null, or empty string concats the addressed columns without
	 * a filler.
	 * 
	 * If tuple-to-fuse-in is null, return null.
	 * If tuple-to-fuse-in is empty, return empty string
	 */
	@Override
	public String exec(Tuple input) throws IOException {

		try {
			if (input == null || input.size() < 3) 
				return null;

			// Check args:
			String sliceDecl = (String) input.get(SLICE_SPEC_POS);
			String concatSeparator = (String) input.get(CONCAT_SEPARATOR_POS);
			if (concatSeparator == null)
				concatSeparator = "";
			DefaultTuple tupleToFuse = (DefaultTuple) input.get(TUPLE_TO_FUSE_POS);
			
			if (tupleToFuse == null)
				return null;
			
			if (tupleToFuse.size() == 0)
				return "";
			
			String[] startEnd = sliceDecl.split(":");
			

			// Parse and error check the slice declaration:
			int start;
			int end;
			
			// For slice def of ":", startEnd will be empty:
			if (startEnd.length == 0) {
				startEnd = new String[2];
				startEnd[0] = "0";
				startEnd[1] = ((Integer)tupleToFuse.size()).toString();
			} else if (startEnd.length == 1) {
				// Spec was "i:". For this input, split leads to single-field array [i], where i is start:
					String theStartStr = startEnd[0];
					startEnd = new String[2];
					startEnd[0] = theStartStr;
					startEnd[1] = ((Integer)tupleToFuse.size()).toString();
			}				
				
			if (startEnd[0].length() == 0)
				start = 0;
			else
				start = Integer.valueOf(startEnd[0]);
			end = Integer.valueOf(startEnd[1]);
			
			// Now have start and end as integers. Handle negative specs:
			if (start < 0)
				start = tupleToFuse.size() + start;
			if (end < 0)
				end = tupleToFuse.size() + end;
			
			if (end > tupleToFuse.size())
				// Be tolerant: Treat end > tuple size as
				// "get all from start to end of tuple):
				end = tupleToFuse.size();
			
			if (start > end)
				throw new InvalidParameterException("In FuseColumns, slice spec start must be <= end.");
			
			if (start == end)
				return (String) tupleToFuse.get(start);
			
			if (start > tupleToFuse.size() - 1 )
				throw new InvalidParameterException("In FuseColumns, slice spec start must be < number of fields.");
				
			// Finally, all seems kosher:
			String result = "";
			for (int indx=start; indx<end; indx++) {
				result += tupleToFuse.get(indx) + concatSeparator;
			}
			// Snip off the trailing concatSeparator, if it's not empty:
			if (concatSeparator.length() == 0)
				return result.substring(0,result.length());
			else
				return result.substring(0,result.length() - 1);
			
		} catch (Exception e) {
			throw new IOException("Exception ConcatColumns().", e);
		}
	}
	
	public Schema outputSchema(Schema input) {
        try{
            return new Schema(input.getField(0));
        }catch (Exception e){
                return null;
        }
    }    
}
