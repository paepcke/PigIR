package edu.stanford.pigir.pigudf;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * @author paepcke
 * Pig Filter function: Receives a CSV string and returns true 
 * if all fields are longer than a given minimum, and shorter
 * than a given maximum. Call: CSVMinMaxLength(csvStr, min, max).
 * Return: boolean.
 */

public class CSVMinMaxLength extends FilterFunc {

	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() < 3)
			return null;
		String value = null;
		Integer minSize  = -1;
		Integer maxSize  = -1;
		try {
			value   = (String)input.get(0);
			minSize = (Integer)input.get(1);
			maxSize = (Integer)input.get(2);
		} catch (ExecException ee) {
			throw new IOException("Caught exception processing input row ", ee);
		}
		String[] csvFields = value.split(",");
		for (int i=0; i<csvFields.length; i++) {
		    if ((maxSize > -1) && (csvFields[i].length() > maxSize))
			return false;
		    if ((minSize > -1) && (csvFields[i].length() < minSize))
			return false;
		}
		return true;
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Tuple testTuple;
		
		CSVMinMaxLength lenChecker = new CSVMinMaxLength();
		testTuple = TupleFactory.getInstance().newTuple(3);
		testTuple.set(0, "foo,bar");
		testTuple.set(1,1);
		testTuple.set(2,4);
		if (!lenChecker.exec(testTuple)) {
			throw new IOException("foo and bar deemed longer than 4 or shorter than 1");
		}
		
		testTuple = TupleFactory.getInstance().newTuple(3);
		testTuple.set(0, "foo,bar");
		testTuple.set(1, 1);
		testTuple.set(2, 2);
		if (lenChecker.exec(testTuple)) {
			throw new IOException("foo and bar deemed shorter than 2 or larger than 1");
		}
		
		testTuple = TupleFactory.getInstance().newTuple(3);
		testTuple.set(0, "a,super");
		testTuple.set(1, 1);
		testTuple.set(2, 5);
		if (!lenChecker.exec(testTuple)) {
			throw new IOException("a and super deemed longer than 5 or shorter than 4");
		}
		
		testTuple = TupleFactory.getInstance().newTuple(3);
		testTuple.set(0, "a,super");
		testTuple.set(1, 3);
		testTuple.set(2, 4);
		if (lenChecker.exec(testTuple)) {
			throw new IOException("a and super deemed shorter than 4 or less than 3");
		}

		testTuple = TupleFactory.getInstance().newTuple(3);
		testTuple.set(0, "super,duper");
		testTuple.set(1, 5);
		testTuple.set(2, 5);
		if (!lenChecker.exec(testTuple)) {
			throw new IOException("super and duper deemed shorter than 5 or longer than 5");
		}
		
		testTuple = TupleFactory.getInstance().newTuple(3);
		testTuple.set(0, "super,a");
		testTuple.set(1, 2);
		testTuple.set(2, 5);
		if (lenChecker.exec(testTuple)) {
			throw new IOException("super and 'a' deemed less than 5 or longer than 2");
		}
		
		
		
		System.out.println("All good with CSVLenNumeric.");
	}
}
