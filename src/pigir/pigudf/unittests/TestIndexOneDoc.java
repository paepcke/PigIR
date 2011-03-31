 package pigir.pigudf.unittests;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import pigir.pigudf.GetUUID;
import pigir.pigudf.IndexOneDoc;

public class TestIndexOneDoc {
	
	class Truth {
		public String word;
		public int pos;
		
		public Truth(String theWord, int thePos) {
			word = theWord;
			pos = thePos;
		}
	}
	
	
	/**
	 * For each result from IndexOneDoc, verify that every tuple is correct.
	 * 
	 * @param result is a bag: {(docID), (token1,docID,token1Pos), (token2,docID,token2Pos), ...}. All docID are identical. 
	 * @param groundTruth an array of Truth objects. Each object contains one token and its position. The objects are ordered as in the expected result.
	 * @return true/false.
	 */
	private static boolean matchOutput(DataBag result, ArrayList<Truth> groundTruth) {

		Iterator<Tuple> resultIt = result.iterator();
		Iterator<Truth> truthIt  = groundTruth.iterator();
		Tuple nextRes = null;
		Truth nextTruth = null;
		String bagDocID = null;

		try {

			if (result.size() == 0 && groundTruth.size() == 0)
				return true;
			
			// Get the bag docid:
			bagDocID = (String) resultIt.next().get(0);
			
			while (resultIt.hasNext()) {
				if (! truthIt.hasNext())
					return false;
				nextRes   = resultIt.next();
				nextTruth = truthIt.next();
				if (!nextRes.get(0).equals(nextTruth.word) || !nextRes.get(1).equals(bagDocID) || !nextRes.get(2).equals(nextTruth.pos))
					return false;
			}
			if (truthIt.hasNext())
				return false;

			return true;
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws ExecException {
		
		IndexOneDoc func = new IndexOneDoc();
		final TestIndexOneDoc tester = new TestIndexOneDoc();
		final int contentIndex = 1;
		
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(2);
		parms.set(0, GetUUID.newUUID());
		
		try {
			// System.out.println(func.outputSchema(new Schema()));
			// Simple, straight forward:

			parms.set(1, "On a sunny day");
			assertTrue(matchOutput(func.exec(parms), new ArrayList<Truth>() {
				{
					add(tester.new Truth("sunny", 2));
					add(tester.new Truth("day", 3));
				};
			}));
			
			// Embedded URL:
			parms.set(contentIndex, "On a http://infolab.stanford.edu/~user sunny day.");
			assertTrue(matchOutput(func.exec(parms), new ArrayList<Truth>() {
				{
					add(tester.new Truth("http://infolab.stanford.edu/~user", 2));
					add(tester.new Truth("sunny", 3));
					add(tester.new Truth("day", 4));
				};
			}));
			

			// Just a URL:
			parms.set(contentIndex, "ftps://my.domain/");
			assertTrue(matchOutput(func.exec(parms), new ArrayList<Truth>() {
				{
					add(tester.new Truth("ftps://my.domain/", 0));
				};
			}));

			// Empty string:
			parms.set(contentIndex, "");
			assertTrue(matchOutput(func.exec(parms), new ArrayList<Truth>()));


			// Don't ignore stopwords:
			Tuple input = tupleFac.newTuple();
			input.append("docID");
			input.append("The sun is shining.");
			input.append(0); // no stopwords
						
			assertTrue(matchOutput(func.exec(input), new ArrayList<Truth>() {
				{
					add(tester.new Truth("The", 0));
					add(tester.new Truth("sun", 1));
					add(tester.new Truth("is", 2));
					add(tester.new Truth("shining", 3));
				};
			}));

			// Don't ignore stopwords and use SPACE as token delimiter:
			input = tupleFac.newTuple();
			input.append("docID");
			input.append("The sun is shining.");
			input.append(0); // no stopwords
			input.append(null); // default URL preservation
			input.append(" "); // SPACE as delimiter
						
			assertTrue(matchOutput(func.exec(input), new ArrayList<Truth>() {
				{
					add(tester.new Truth("The", 0));
					add(tester.new Truth("sun", 1));
					add(tester.new Truth("is", 2));
					add(tester.new Truth("shining.", 3));
				};
			}));
			
			System.out.println("Output schema: " + func.outputSchema(null));
			System.out.println("All tests passed.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
