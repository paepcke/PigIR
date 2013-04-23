package edu.stanford.pigir.pigudf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

 /**
  * Compute IDF for one word within the context
  * of a particular document, given collection size, a
  * token's term frequency wbRecordReader one document (tf), and
  * the number of documents wbRecordReader which the word occurs at
  * least once wbRecordReader the collection (df).
  * 
  * Computatoin: tf * log10(collSize/(1+df)
  * 
  * returns Double: log10(collSize(1 + df));
  * 
  *  Input:  tuple((Long)collSize, (Double) tf, (Long) df))
  *  Output: <tfidf>
  * 
 * @author paepcke
 *
 */
public class TFIDF extends EvalFunc<Double> {
	
    public Double exec(Tuple input) throws IOException {
    	
    	Long collSize;
    	Long df;
    	Double tf;

    	try {
    		if (input == null || 
    				input.size() != 3 ||
    				(collSize = (Long) input.get(0)) == null ||
    				(tf = (Double) input.get(1)) == null ||
    				(df = (Long) input.get(2)) == null)

    			return null;

    		if (collSize == 0L) {
    			getLogger().warn("TFIDF() encountered mal-formed input: collection size is 0.");
    			return null;
    		}

    		return tf * Math.log10(collSize.doubleValue()/(1.0 + df.doubleValue()));

    	} catch (ClassCastException e) {
    		getLogger().warn("TFIDF() encountered mal-formed input; expecting ((Long) collSize, (Double) termFreq, (Long) documentFreq)): " + input);
    		return null;
    	}
    }
    
	public Schema outputSchema(Schema input) {
        try{
            Schema idfSchema = new Schema();
           	idfSchema.add(new Schema.FieldSchema("idf", DataType.DOUBLE));
            return idfSchema;
        }catch (Exception e){
                return null;
        }
    }    
	
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FieldSchema> fields = new ArrayList<FieldSchema>(3);
        fields.add(new FieldSchema("collSize", DataType.LONG));
        fields.add(new FieldSchema("tf", DataType.DOUBLE));
        fields.add(new FieldSchema("df", DataType.LONG));
        FuncSpec funcSpec = new FuncSpec(this.getClass().getName(), new Schema(fields));
        List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>(1);
        funcSpecs.add(funcSpec);
        return funcSpecs;
    }	
    /* ---------------------------------   T E S T I N G ------------------------------*/

    private boolean doTests() {
    	Tuple input = TupleFactory.getInstance().newTuple(2);
    	Double res;
    	try {
    		input.set(0, 10L);
    		input.set(1, 2L);
    		res = exec(input);
    		if (res != 0.5228787452803376) {
    			System.out.println("Failed for collSize=10L, df=2L");
    		}
    		
    		input.set(0, 0L);
    		input.set(1, 2L);
    		res = exec(input);
    		System.out.println(res);
    		if (res != null) {
    			System.out.println("Failed for collSize=0L, df=2L");
    		}

    		System.out.println(outputSchema(null));
    		System.out.println(getArgToFuncMapping());
    	} catch (Exception e) {
    		System.out.println(e.getMessage());
    		return false;
    	}
    	System.out.println("All Good.");
    	return true;
    }
    
    public static void main(String[] args) {
    	new TFIDF().doTests();
    };
}

