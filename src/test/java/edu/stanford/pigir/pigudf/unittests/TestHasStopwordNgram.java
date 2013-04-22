package edu.stanford.pigir.pigudf.unittests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BinSedesTupleFactory;
import org.apache.pig.data.Tuple;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.pigir.pigudf.HasStopwordNgram;

public class TestHasStopwordNgram {

	public static BinSedesTupleFactory tupleFac = null;
	Tuple parmLen1 = null;
	Tuple parmLen2 = null;
	HasStopwordNgram ngramTestFunc = null; 	
	
	static class TupleFac{
		public static Tuple newTuple(String[] content) throws ExecException {
			Tuple t = TestHasStopwordNgram.tupleFac.newTuple(content.length);
			for (int i=0; i<content.length; i++)
				t.set(i, content[i]);
			return t;
		}
	}
	
	@Before
	public void setUp() throws Exception {
		tupleFac = new BinSedesTupleFactory();
		parmLen1 = tupleFac.newTuple(1);
		parmLen2 = tupleFac.newTuple(2);
		ngramTestFunc = new HasStopwordNgram(); 

	}
	
	@Test
	public void testNgrams() throws IOException {
		assertFalse(ngramTestFunc.exec(TupleFac.newTuple(new String[] {"rumble"})));
		assertFalse(ngramTestFunc.exec(TupleFac.newTuple(new String[] {"juicy","test"})));
		assertFalse(ngramTestFunc.exec(TupleFac.newTuple(new String[] {"juicy","test","food"})));
		assertFalse(ngramTestFunc.exec(TupleFac.newTuple(new String[] {"juicy","test","food","dog"})));
		assertFalse(ngramTestFunc.exec(TupleFac.newTuple(new String[] {"juicy","test","go","running","building"})));

		assertTrue(ngramTestFunc.exec(TupleFac.newTuple(new String[] {"a"})));
		assertTrue(ngramTestFunc.exec(TupleFac.newTuple(new String[] {"a","this"})));
		assertTrue(ngramTestFunc.exec(TupleFac.newTuple(new String[] {"juice","garden","this"})));
	}
}
