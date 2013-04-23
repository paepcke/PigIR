package edu.stanford.pigir.pigudf.unittests;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BinSedesTupleFactory;
import org.apache.pig.data.Tuple;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.pigir.pigudf.AllProperLengthNgram;

public class TestAllProperLengthNgram {

	public static BinSedesTupleFactory tupleFac = null;
	Tuple parmLen1 = null;
	Tuple parmLen2 = null;
	AllProperLengthNgram ngramTestFunc = null; 	
	
	static class TupleFac{
		public static Tuple newTuple(int minLength, int maxLength, String[] content) throws ExecException {
			Tuple t = TestAllProperLengthNgram.tupleFac.newTuple(2 + content.length);
			t.set(0, minLength);
			t.set(1, maxLength);
			for (int i=0; i<content.length; i++)
				t.set(i+2, content[i]);
			return t;
		}
	}
	
	@Before
	public void setUp() throws Exception {
		tupleFac = new BinSedesTupleFactory();
		parmLen1 = tupleFac.newTuple(1);
		parmLen2 = tupleFac.newTuple(2);
		ngramTestFunc = new AllProperLengthNgram(); 

	}
	
	@Test
	public void testNgrams() throws IOException {
		assertTrue(ngramTestFunc.exec(TupleFac.newTuple(2,6,new String[] {"rumble"})));
		assertFalse(ngramTestFunc.exec(TupleFac.newTuple(2,5,new String[] {"rumble"})));
		assertFalse(ngramTestFunc.exec(TupleFac.newTuple(-1,5,new String[] {"rumble"})));
		assertTrue(ngramTestFunc.exec(TupleFac.newTuple(-1,-1,new String[] {"rumble"})));
		assertTrue(ngramTestFunc.exec(TupleFac.newTuple(4,-1,new String[] {"rumble"})));
		assertTrue(ngramTestFunc.exec(TupleFac.newTuple(4,-1,new String[] {"rumble","bluetooth"})));
		assertFalse(ngramTestFunc.exec(TupleFac.newTuple(4,-1,new String[] {"so","bluetooth"})));
		
		// Test having a comma-separated list as the 3rd field, rather than having 
		// the words of the ngram spread across various tuple fields:
		assertTrue(ngramTestFunc.exec(TupleFac.newTuple(4,-1,new String[] {"rumble,bluetooth"})));
		assertFalse(ngramTestFunc.exec(TupleFac.newTuple(4,-1,new String[] {"so,bluetooth"})));
	}
}
