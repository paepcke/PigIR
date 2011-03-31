package pigir.pigudf.unittests;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import pigir.pigudf.First;

public class TestFirst {
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws ExecException {
		
		
		final TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple tstTuple;
		First func = new First();
		
		try {
			tstTuple = tupleFac.newTuple(new ArrayList<Integer>() {
				{
					add(0, 0);
					add(1, 1);
					add(2, 2);
				
				};
			});

			assertTrue(new Integer(0).equals(func.exec(tstTuple)));
			
			//-----------
			tstTuple = tupleFac.newTuple(new ArrayList<String>() {
				{
					add(0, "one");
					add(1, "two");
					add(2, "three");
				
				};
			});

			assertTrue("one".equals(func.exec(tstTuple)));
			
			//-----------
			final ArrayList<String> firstEl = new ArrayList<String>() {
				{
					add("foo"); 
					add("bar");
				};
			};
			
			final ArrayList<Integer> secEl = new ArrayList<Integer>() {
				{
					add(0); 
					add(1);
				};
			};
			
			tstTuple = tupleFac.newTuple(new ArrayList<Tuple>() {
				{
					add(0, tupleFac.newTuple(firstEl));
					add(1, tupleFac.newTuple(secEl));
				
				};
			});

			Tuple res = (Tuple) func.exec(tstTuple);
			for (int i=0; i<firstEl.size(); i++) {
				assertTrue(firstEl.get(i).equals(res.get(i)));
			}
			
			System.out.println("All tests passed.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
