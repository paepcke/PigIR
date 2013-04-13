package edu.stanford.pigir.pigudf.unittests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BinSedesTupleFactory;
import org.apache.pig.data.Tuple;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.pigir.pigudf.StripHTML;

public class TestStripHTML {

	BinSedesTupleFactory tupleFac = null;
	Tuple parm = null;
	StripHTML stripFunc = null;
	
	@Before
	public void setUp() throws Exception {
		tupleFac = new BinSedesTupleFactory();
		parm = tupleFac.newTuple(1);
		stripFunc = new StripHTML();
	}
	
	private Tuple newTup(String text) throws ExecException {
		Tuple t = tupleFac.newTuple(2);
		t.set(0, text);
		t.set(1, text.length());
		return t;
				
	}

	@Test
	public void test() throws IOException {
		parm.set(0, "<html><head></head><body>Foobar</body></html>");
		assertEquals(newTup("Foobar"), stripFunc.exec(parm));
		
		parm.set(0, "<html><head></head><body><br>Foobar</br></body></html>");
		assertEquals(newTup("Foobar"), stripFunc.exec(parm));
		
		parm.set(0, "<html><head></head><body><br><a src=http://blue/bell>Foobar</a></br></body></html>");
		assertEquals(newTup("Foobar"), stripFunc.exec(parm));

		parm.set(0, "<html><head></head><body>this &amp; that</body></html>");
		assertEquals(newTup("this & that"), stripFunc.exec(parm));
		
		parm.set(0, "<html><head></head><body>10&lt;11</body></html>");
		assertEquals(newTup("10<11"), stripFunc.exec(parm));
		
		parm.set(0, "<html><head></head><body>&quot;You&quot;</body></html>");
		assertEquals(newTup("\"You\""), stripFunc.exec(parm));
		
		parm.set(0, "<html><head></head><body>Me&nbsp;You</body></html>");
		assertEquals(newTup("Me You"), stripFunc.exec(parm));
		
		parm.set(0, "<html><head></head><body>You&copy;</body></html>");
		assertEquals(newTup("You©"), stripFunc.exec(parm));
		
		parm.set(0, "<html><head></head><body>You&reg;</body></html>");
		assertEquals(newTup("You®"), stripFunc.exec(parm));

		parm.set(0, "<html><head></head><body>&bull;You</body></html>");
		assertEquals(newTup("•You"), stripFunc.exec(parm));

		parm.set(0, "<html><head></head><body>You&#169Me</body></html>");
		assertEquals(newTup("You©Me"), stripFunc.exec(parm));
	}
}

	

