package edu.stanford.pigir.pigudf;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class IsNumber extends FilterFunc {
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		String value = null;
		try {
			value = (String)input.get(0);
		} catch (ExecException ee) {
			throw new IOException("Caught exception processing input row ", ee);
		}
		if (isNumeric(value)) {
			return true;
		} else {
			return false;
		}
	}
    
	public static boolean isNumeric(String str) {
	    for (char c : str.toCharArray()) {
	        if (c != '.' && c != '-' && c != '+' & !Character.isDigit(c)) return false;
	    }
	    return true;
	}    

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Tuple testTuple;
		
		IsNumber numChecker = new IsNumber();
		ArrayList<String> input = new ArrayList<String>();
		input.add("10");
		testTuple = TupleFactory.getInstance().newTuple(input);
		if (!numChecker.exec(testTuple)) {
			throw new IOException("10 was not recognized as a number.");
		}
		input.clear();
		input.add("30.5");
		testTuple = TupleFactory.getInstance().newTuple(input);
		if (!numChecker.exec(testTuple)) {
			throw new IOException("30.5 was not recognized as a number.");
		}
		input.clear();
		input.add("+100");
		testTuple = TupleFactory.getInstance().newTuple(input);
		if (!numChecker.exec(testTuple)) {
			throw new IOException("+100 was not recognized as a number.");
		}
		input.clear();
		input.add("-100");
		testTuple = TupleFactory.getInstance().newTuple(input);
		if (!numChecker.exec(testTuple)) {
			throw new IOException("-100 was not recognized as a number.");
		}
		input.clear();
		input.add("foo.bar");
		testTuple = TupleFactory.getInstance().newTuple(input);
		if (numChecker.exec(testTuple)) {
			throw new IOException("recognized foo.bar as a number.");
		}
		System.out.println("All good with IsNumeric.");
	}
}
