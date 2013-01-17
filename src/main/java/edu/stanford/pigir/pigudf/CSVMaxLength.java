package edu.stanford.pigir.pigudf;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * @author paepcke
 * Pig Filter function: Receives a CSV string and returns true 
 * if all fields are composed of only alpha chars.
 */

public class CSVMaxLength extends FilterFunc {

	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() < 2)
			return null;
		String value = null;
		Integer maxSize  = -1;
		try {
			value   = (String)input.get(0);
			maxSize = (Integer)input.get(1);
		} catch (ExecException ee) {
			throw new IOException("Caught exception processing input row ", ee);
		}
		String[] csvFields = value.split(",");
		for (int i=0; i<csvFields.length; i++) 
			if (csvFields[i].length() > maxSize)
				return false;
		return true;
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Tuple testTuple;
		
		CSVMaxLength lenChecker = new CSVMaxLength();
		testTuple = TupleFactory.getInstance().newTuple(2);
		testTuple.set(0, "foo,bar");
		testTuple.set(1, 4);
		if (!lenChecker.exec(testTuple)) {
			throw new IOException("foo and bar deemed longer than 4");
		}
		
		testTuple = TupleFactory.getInstance().newTuple(2);
		testTuple.set(0, "foo,bar");
		testTuple.set(1, 2);
		if (lenChecker.exec(testTuple)) {
			throw new IOException("foo and bar deemed shorter than 2");
		}
		
		testTuple = TupleFactory.getInstance().newTuple(2);
		testTuple.set(0, "a,super");
		testTuple.set(1, 5);
		if (!lenChecker.exec(testTuple)) {
			throw new IOException("a and super deemed longer than 5");
		}
		
		testTuple = TupleFactory.getInstance().newTuple(2);
		testTuple.set(0, "a,super");
		testTuple.set(1, 4);
		if (lenChecker.exec(testTuple)) {
			throw new IOException("a and super deemed shorter than 4");
		}

		System.out.println("All good with CSVLenNumeric.");
	}
}
