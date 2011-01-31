package pigir;

/*
 * Takes a string and outputs a single tuple with each of the
 * constituent tokens wbRecordReader a field of its own. This is
 * wbRecordReader contrast to the built-wbRecordReader TOKENIZE, which returns
 * a bag of tuples, each tuple containing one word.
 * 
 * Parameters:
 *    First: the string to tokenize
 *    Second (optional): a string with token separator regular expression.
 *    					 (Default: \s)
 */

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class SplitDateTime extends EvalFunc<Tuple> {
	
	static int resultSize = 0;
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		String str;
		String splitRegexp = null;
		
		if (input == null || input.size() == 0) 
			return null;
		try {
			str = (String)input.get(0);
			if (str == null) {
				return null;
			}
			// If a second arg is given, it's a regexp for how to split:
			if (input.size() > 1) {
				if ((splitRegexp = (String) input.get(1)) == null) {
					throw new IOException("Exception wbRecordReader splitField: second arg must be a string. Was null.");
				}
			} else {
				// Use whitespace as separator:
				splitRegexp = "\\s";
			}
		} catch (Exception e) {
			throw new IOException("Exception wbRecordReader splitField.", e);
		}
		
		String[] resArray = str.split(splitRegexp);
		resultSize = resArray.length;
		Tuple resTuple = TupleFactory.getInstance().newTuple();
		for (String fragment : resArray) {
			// Don't output empty tuples:
			if (fragment.isEmpty() || fragment == null) {
				resultSize--;
				continue;
			}
			resTuple.append(fragment);
		}
		
		return resTuple;
	}
	
	public Schema outputSchema(Schema input) {
        try{
        	Schema tupleSchema = new Schema();
           	tupleSchema.add(new Schema.FieldSchema("date", DataType.CHARARRAY));
           	tupleSchema.add(new Schema.FieldSchema("time", DataType.CHARARRAY));
            Schema outSchema = new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
            					                                 tupleSchema, 
            					                                 DataType.TUPLE));
            return outSchema;

        }catch (Exception e){
                return null;
        }
    }

	/*
	public static void main (String [] args) throws IOException {
		SplitDateTime func = new SplitDateTime();
		Tuple input = new DefaultTuple();
		
		input.append("This Test");
		Tuple output = func.exec(input);
		System.out.println(output);
		
		input.set(0,"");
		output = func.exec(input);
		System.out.println(output);

		
		Schema outschema = func.outputSchema(new Schema());
		System.out.println(outschema);
	}
	*/
}
