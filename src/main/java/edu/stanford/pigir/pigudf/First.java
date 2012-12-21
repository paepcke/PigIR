package edu.stanford.pigir.pigudf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Returns the first element of a tuple
 * 
 * @author Andreas Paepcke
 * @see rest()
 */
public class First extends EvalFunc<Object> {
	
	@Override
	public Object exec(Tuple input) throws IOException {

		
		try {
			if (input == null || input.size() == 0) 
				return null;
			return (Object) input.get(0);
		} catch (Exception e) {
			throw new IOException("Exception First().", e);
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
