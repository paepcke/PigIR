package pigir.pigudf;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Remove debris from either, or both sides of a given
 * string. The definition of 'debris' is a regular expression.
 * This method is thus a generalized trim().
 * @author paepcke
 *
 * Revisions:
 * 	- Replaced chomp(String,String) with removedEnd(String,String) as the former is deprecated in org.apache.lang3. Dec 14, 2012
 */
public class ChopChars extends EvalFunc<String> {
	
	public static enum WHICH_SIDE {
		BOTH,     // In calls from Pig use 0
		LEFT,     // In calls from Pig use 1
		RIGHT     // In calls from Pig use 2
	}
	
	private final int BOTH_SIDES = 0;
	private final int LEFT_SIDE = 1;
	private final int RIGHT_SIDE = 2;
	
	static final Pattern DEFAULT_PATTERN = Pattern.compile("([\\p{Punct}{}/\\\\]*)([^\\p{Punct}]*)([\\p{Punct}{}/\\\\]*)");
	
	@Override
	public String exec(Tuple input) throws IOException {
		String str;
		String strToChop = null;
		Boolean sidesSpecGiven = false;
		Integer choice = BOTH_SIDES;
		
		if (input == null || input.size() == 0) 
			return null;
		try {
			// Get the word to chomp:
			str = (String)input.get(0);
			if (str == null) {
				return null;
			}
			// If a second arg is given, it's the choice of whether to chop
			// of both sides of the word, left, or right:
			sidesSpecGiven = ((input.size() > 1) && (choice = (Integer) input.get(1)) != null);
			if (choice == null)
				choice = BOTH_SIDES;
				
			// If a third arg is given, it's a regexp for what to chop off.
			// In that case the second arg (sides) is irrelevant:
			if (input.size() > 2 && (strToChop = (String) input.get(2)) != null) {
				return ChopChars.chomp(str, strToChop);
			}
			
		} catch (java.lang.ClassCastException e) {
			throw new IOException("Usage: ChopChars(String str, [[Integer chompSideChoice], String regexp]\n" + e.getMessage());
		} catch (Exception e) {
			throw new IOException("Exception wbRecordReader chopChars.", e);
		}
			
		if (sidesSpecGiven) {
			switch (choice) {
			case BOTH_SIDES:
				return ChopChars.chomp(str, WHICH_SIDE.BOTH);
			case LEFT_SIDE:
				return ChopChars.chomp(str, WHICH_SIDE.LEFT);
			case RIGHT_SIDE:
				return ChopChars.chomp(str, WHICH_SIDE.RIGHT);
			default:
				throw new IOException("Chomp side parameter must be " + BOTH_SIDES + "(both sides)" + "," +
						LEFT_SIDE + " (chomp left side), or " +
						RIGHT_SIDE + " (chomp right side). Was: " + choice);
			}
		}
		return str;
	}
	
	static String chomp(String str) {
		return ChopChars.chomp(str, ChopChars.WHICH_SIDE.BOTH);
	}
	
	static String chomp(String str, WHICH_SIDE where) {
		return ChopChars.chomp(str, where, ChopChars.DEFAULT_PATTERN);
	}
	
	static String chomp (String str, WHICH_SIDE where, Pattern chopPattern) {
		
		Matcher thisMatcher = chopPattern.matcher(str);
		if (thisMatcher.matches()) {
			if (where.equals(WHICH_SIDE.BOTH))
				return thisMatcher.group(2);
			if (where.equals(WHICH_SIDE.LEFT))
				return thisMatcher.group(2) + thisMatcher.group(3);
			else return thisMatcher.group(1) + thisMatcher.group(2);
		} else return str;
	}

	static String chomp (String str, String regexp) {
		return org.apache.commons.lang3.StringUtils. removeEnd(str, regexp);
	}
	
	public static void main(String[] argv) {
		Tuple tuple1Parm = TupleFactory.getInstance().newTuple(1);
		Tuple tuple2Parms = TupleFactory.getInstance().newTuple(2);
		Tuple tuple3Parms = TupleFactory.getInstance().newTuple(3);
		
		try {
			//--------------  Chomping without UDF Mechanics -----------
			
			System.out.println("****** Chomping without UDF mechanics:");
			
			System.out.println("'Foobar' both => 'Foobar': '" + ChopChars.chomp("Foobar") + "'");
			System.out.println("'Foobar.' both => 'Foobar': '" + ChopChars.chomp("Foobar.") + "'");
			System.out.println("'.Foobar.' both => 'Foobar': '" + ChopChars.chomp(".Foobar.") + "'");
			System.out.println("'?.Foobar!' both => 'Foobar': '" + ChopChars.chomp("?.Foobar!") + "'");
			System.out.println("'<the works>:?!{}~/\\' both => 'the works': '" + ChopChars.chomp("<the works>:?!{}~/\\") + "'");
			
			System.out.println("'?.Foobar!' right => '?.Foobar': '" + ChopChars.chomp("?.Foobar!", ChopChars.WHICH_SIDE.RIGHT) + "'");
			System.out.println("'?.Foobar!' left => 'Foobar!': '" + ChopChars.chomp("?.Foobar!", ChopChars.WHICH_SIDE.LEFT) + "'");
						
			//--------------  UDF Mechanics One Parameter (String only) -----------
			
			System.out.println("****** Chomping via UDF one parameter:");
			
			tuple1Parm.set(0, "foobar");
			ChopChars func = new ChopChars();
			System.out.println("'foobar' both  => 'foobar': " + func.exec(tuple1Parm));
			
			tuple1Parm.set(0, "<the works>:?!{}~/\\");
			System.out.println("'<the works>:?!{}~/\\' both  => 'the works': " + func.exec(tuple1Parm));
			
			//--------------  UDF Mechanics Two Parameters (String and left/right/both chomp sides) -----------			
						
			System.out.println("****** Chomping via UDF two parameters (str and sides):");			
			
			tuple2Parms.set(0, "foobar");
			tuple2Parms.set(1, 0); //both
			System.out.println("'foobar'  => 'foobar': " + func.exec(tuple2Parms));

			tuple2Parms.set(0, ".?!foobar'[");
			tuple2Parms.set(1, 0); //both
			System.out.println("'.?!foobar\'[' both  => 'foobar': " + func.exec(tuple2Parms));
			
			tuple2Parms.set(1, 1); //left
			System.out.println("'.?!foobar\'[' left => 'foobar\'[': " + func.exec(tuple2Parms));
			
			tuple2Parms.set(1, 2); //right
			System.out.println("'.?!foobar\'[' right => '.?!foobar': " + func.exec(tuple2Parms));
			
			tuple2Parms.set(0, "<the works>:?!{}~/\\");
			tuple2Parms.set(1, 0); //both
			System.out.println("'<the works>:?!{}~/\\'  both => 'the works>': " + func.exec(tuple2Parms));
			
			tuple2Parms.set(0, "<the works>:?!{}~/\\");
			tuple2Parms.set(1, 1); // left
			System.out.println("'<the works>:?!{}~/\\' left => 'the works>:?!{}~/\\': " + func.exec(tuple2Parms));
			
			tuple2Parms.set(0, "<the works>:?!{}~/\\");
			tuple2Parms.set(1, 2); // right
			System.out.println("'<the works>:?!{}~/\\' right => '<the works': " + func.exec(tuple2Parms));
			
			tuple2Parms.set(0, "<the works>:?!{}~/\\");
			tuple2Parms.set(1, null); //both
			System.out.println("'<the works>:?!{}~/\\' null => '<the works>:?!{}~/\\': " + func.exec(tuple2Parms));
			
			//--------------  UDF Mechanics Two Parameters (String, null for left/right/both chomp sides, and regexp) -----------						
			
			System.out.println("****** Chomping via UDF three parameters (str, null, and regexp):");			
			
			tuple3Parms.set(0, "foobar");
			tuple3Parms.set(1, null);
			tuple3Parms.set(2, "foo");
			System.out.println("'foobar' null => 'foobar': " + func.exec(tuple2Parms));

			tuple3Parms.set(0, "bluebell");
			tuple3Parms.set(1, null);
			tuple3Parms.set(2, "foo");
			System.out.println("'bluebell' null => 'bluebell': " + func.exec(tuple3Parms)); // bluebell
			

			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}	
}


