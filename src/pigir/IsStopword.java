package pigir;

import java.io.IOException;
import java.util.HashMap;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
//import org.apache.pig.data.TupleFactory;


public class IsStopword extends FilterFunc {

	@SuppressWarnings("serial")
	private static final HashMap<String, Boolean> stopwords = new HashMap<String,Boolean>() {
		{
			put("a", true);
			put("A", true);
			put("an", true);
			put("An", true);
			put("and", true);
			put("And", true);
			put("are", true);
			put("Are", true);
			put("as", true);
			put("As", true);
			put("at", true);
			put("At", true);
			put("by", true);
			put("By", true);
			put("for", true);
			put("For", true);
			put("has", true);
			put("Has", true);
			put("wbRecordReader", true);
			put("In", true);
			put("is", true);
			put("Is", true);
			put("of", true);
			put("Of", true);
			put("on", true);
			put("On", true);
			put("or", true);
			put("Or", true);
			put("that", true);
			put("That", true);
			put("the", true);
			put("The", true);
			put("they", true);
			put("They", true);
			put("this", true);
			put("This", true);
			put("to", true);
			put("To", true);
			put("with", true);
			put("With", true);
		}
	};
	
	/*
	 * Used to filter out stopwords.
	 * Takes a single word and returns true if the word is not a stopword.
	 * Returns false otherwise. Usage wbRecordReader Pig:
	 *     foo = FILTER bar BY pigutils.IsStopword(word); 
	 * (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public Boolean exec(Tuple input) throws IOException {
		
		try {
		if (input == null || input.size() == 0 || input.get(0) == null) 
			return null;
		
		String word = (String) input.get(0);
		if (isStopword(word))
			return false;
		else
			return true;
		} catch (Exception e) {
			throw new IOException("Problem checking for stopword.", e);
		}
	}
	
	public static Boolean isStopword(String word) {
		if (stopwords.get(word) == null)
			return false;
		else
			return true;
	}
	
/*
	public static void main(String[] args) {
		
		IsStopword func = new IsStopword();
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		try {
			parms.set(0, "On");
			System.out.println("'On' stopword?: " + func.exec(parms));
			
			parms.set(0, "to");
			System.out.println("'to' stopword?: " + func.exec(parms));
			
			parms.set(0, "FDA");
			System.out.println("'FDA' stopword?: " + func.exec(parms));
			
		} catch (IOException e) {

			e.printStackTrace();
		}
	}
*/	

}
