package edu.stanford.pigir.pigudf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class ConcatColumns  extends EvalFunc<String> {
	
	
	@Override
	public String exec(Tuple input) throws IOException {

		try {
			if (input == null || input.size() < 3) 
				return null;

			String sliceDecl = (String) input.get(0);
			String concatSeparator = (String) input.get(1);
			BinSedesTuple tupleToFuse = (BinSedesTuple) input.get(2);
			
			if (tupleToFuse.size() == 0)
				return "";
			
			String[] startEnd = sliceDecl.split(":");
			
			int start;
			int end;
			if (startEnd[0].length() == 0)
				start = 0;
			else
				start = Integer.getInteger(startEnd[0]);
			
			if (startEnd[1].length() == 0)
				end = input.size();
			else 			
				end   = Integer.getInteger(startEnd[1]);			
			
			// Now have [start,end]. Handle negative specs
			// has working from the end of the tuple:
			if (start < 0)
				start = input.size() + start;
			if (end < 0)
				end = input.size() + end;
			
			if (start > end)
				throw new IOException("In FuseColumns, slice spec start must be <= end.");
			
			if (start == end)
				return (String) tupleToFuse.get(start);
			
			if (start > input.size() - 1 )
				throw new IOException("In FuseColumns, slice spec start must be < number of fields.");
				
			// Finally, all seems kosher:
			String result = "";
			for (int indx=start; indx<end; indx++) {
				result += tupleToFuse.get(indx) + concatSeparator;
			}
			
			return result.substring(0,-1);
			
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
