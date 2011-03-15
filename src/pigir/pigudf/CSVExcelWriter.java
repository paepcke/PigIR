package pigir.pigudf;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.pig.StoreFuncInterface;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


public class CSVExcelWriter extends PigStorage implements StoreFuncInterface {

	final static int COMMA = ',';
	final static int DOUBLE_QUOTE = '\"';
	final static int NEWLINE = '\n';
	
	// Pig Storage with COMMA as delimiter:
	TupleFactory tupleMaker = TupleFactory.getInstance();
	
	public CSVExcelWriter() {
		super(",");
	}
	
    @Override
    public void putNext(Tuple tupleToWrite) throws IOException {
    	ArrayList<Object> mProtoTuple = new ArrayList<Object>();
    	// Do the escaping:
    	for (Object field : tupleToWrite.getAll()) {
    		String fieldStr = field.toString();
    		// Embedded double quotes are replaced by two double quotes:
    		fieldStr = fieldStr.replaceAll("[\"]", "\"\"");
    		// If any commas are in the field, or if we did replace
    		// any double quotes with a pair of double quotes above,
    		// or if the string includes a newline character (\n:0x0A),
    		// then the entire field must be enclosed in double quotes:
    		if ((fieldStr.indexOf(COMMA) != -1) || 
    			(fieldStr.indexOf(DOUBLE_QUOTE) != -1) ||
    			(fieldStr.indexOf(NEWLINE) != -1))  {
    			fieldStr = "\"" + fieldStr + "\"";
    		}
    		mProtoTuple.add(fieldStr);
    	}
    	Tuple resTuple = tupleMaker.newTuple(mProtoTuple);
    	//***********
    	System.out.println("Row: " + resTuple);
    	//***********
    	super.putNext(resTuple);
    }
    
    //------------------------- Testing ------------------------

    /*
    @SuppressWarnings("serial")
	public static void main(String[] args) {
    	CSVExcelWriter writer = new CSVExcelWriter();
    	ArrayList<Object> fieldList = null;
    	
    	fieldList = new ArrayList<Object>() {
    		{
    			add(1);
    			add(2);
    			add(3);
    		}
    	};
    	//System.out.println(writer.putNext(TupleFactory.getInstance().newTuple(fieldList)));
    }
    */
}
