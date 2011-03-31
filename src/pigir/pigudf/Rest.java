package pigir.pigudf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Returns the input tuple with the first element removed.  
 * 
 * @author Andreas Paepcke
 * @see rest()
 */
public class Rest extends EvalFunc<Tuple> {
	
	@Override
	public Tuple exec(Tuple input) throws IOException {

		TupleFactory mTupleFactory = TupleFactory.getInstance();
		
		try {
			if (input == null) 
				return null;
			if  (input.size() == 0)
				return input;
			
			return mTupleFactory.newTuple(input.getAll().subList(1, input.size()));
			
		} catch (Exception e) {
			throw new IOException("Exception Rest().", e);
		}
	}
	
	public Schema outputSchema(Schema input) {
        try{
            return input;
        }catch (Exception e){
                return null;
        }
    }    
}
