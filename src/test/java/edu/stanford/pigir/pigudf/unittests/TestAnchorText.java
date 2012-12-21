package edu.stanford.pigir.pigudf.unittests;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import edu.stanford.pigir.Common;
import edu.stanford.pigir.pigudf.AnchorText;

public class TestAnchorText {

	/**
	 * For each result from IndexOneDoc, verify that every tuple is correct.
	 * 
	 * @param result is a tuple: ((docID,numPostings), (token1,docID,token1Pos), (token2,docID,token2Pos), ...). All docID are identical. 
	 * @param groundTruth an array of Truth objects. Each object contains one token and its position. The objects are ordered as in the expected result.
	 * @return true/false.
	 */
	private static boolean matchOutput(Tuple result, ArrayList<String> groundTruth) {

		Iterator<Object> resultIt = Common.getTupleIterator(result);
		Iterator<String> truthIt  = groundTruth.iterator();
		String nextRes = null;
		String nextTruth = null;

		if (result.size() == 0 && groundTruth.size() == 0)
			return true;
		
		while (resultIt.hasNext()) {
			if (! truthIt.hasNext())
				return false;
			nextRes   = (String) resultIt.next();
			nextTruth = truthIt.next();
			if (!nextRes.equals(nextTruth))
				return false;
		}
		if (truthIt.hasNext())
			return false;

		return true;
	}

	@SuppressWarnings("serial")
	public static void main(String[] args) throws ExecException {
		
		AnchorText func = new AnchorText();
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		
		try {

			// No link, and string shorter than a minimum link's length:
			parms.set(0, "On a sunny day");
			assertNull(func.exec(parms));
			
			// No link, long enough to possibly contain a link:
			parms.set(0, "On a truly sunny day we walked along the beach, and smiled.");
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>()));
			
			// Correct and tightly spaced link:
			parms.set(0, "On a <a href=\"http://foo/bar.html\">sunny</a> day");
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("sunny");
				};
			}));

			// Correct link with spaces:
			parms.set(0, "On a <a href   =   \"http://foo/bar.html\">sunny</a> day");
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("sunny");
				};
			}));
			
			// No quotes around the URL ==> Not taken as a link:
			parms.set(0, "On a <a href=http://foo/bar.html>sunny</a> day");
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>()));

			// Multiple links, and three spaces in second anchor:
			parms.set(0, "On a <a href=\"http://foo/bar.html\">sunny</a> day in <a href=\"https:8090//blue/bar?color=green\">in March   </a> we ran.");
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("sunny");
					add("in March   ");
				};
			}));
			
		} catch (IOException e) {
			System.out.println("Failed with IOException: " + e.getMessage());
			System.exit(-1);
		}
		
		System.out.println("All tests passed.");
	}
}
