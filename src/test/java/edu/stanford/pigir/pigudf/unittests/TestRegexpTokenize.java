package edu.stanford.pigir.pigudf.unittests;

import java.io.IOException;
import static org.junit.Assert.*;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import edu.stanford.pigir.pigudf.RegexpTokenize;


public class TestRegexpTokenize {
	
		static RegexpTokenize func = new RegexpTokenize();
		static TupleFactory tupleFac = TupleFactory.getInstance();
		static Tuple parms = tupleFac.newTuple(1);
		static Tuple parmsTwo = tupleFac.newTuple(2);
		static Tuple parmsThree = tupleFac.newTuple(3);
		static Tuple parmsFour = tupleFac.newTuple(4);
		
		public static void main(String[] args) {
		try {
			//System.out.println("*****With default regexp:");
			
			parms.set(0, "On a sunny day");
			assertEquals("",func.exec(parms).toString(), "{(sunny),(day)}");
			
			parms.set(0, "Testing it!");
			assertEquals("",func.exec(parms).toString(), "{(Testing)}");
			
			parms.set(0, "FDA");
			assertEquals("",func.exec(parms).toString(), "{(FDA)}");
			
			//-------------
			//System.out.println("*****Now with whitespace as regexp:");
			parmsTwo.set(1, "[\\s]");

			parmsTwo.set(0, "On a sunny day");
			assertEquals("",func.exec(parmsTwo).toString(), "{(sunny),(day)}");
			
			parmsTwo.set(0, "Testing it!");
			assertEquals("",func.exec(parmsTwo).toString(), "{(Testing),(it!)}");
			
			parmsTwo.set(0, "FDA");
			assertEquals("",func.exec(parmsTwo).toString(), "{(FDA)}");

			//-------------
			//System.out.println("*****Test stopword elimination:");
			parmsThree.set(1, null);
			parmsThree.set(2, 1);
			
			parmsThree.set(0, "foo");
			assertEquals("",func.exec(parmsThree).toString(), "{(foo)}");
			
			parmsThree.set(0, "This is a stopword test.");
			assertEquals("",func.exec(parmsThree).toString(), "{(stopword),(test)}");
			//-------------
			//System.out.println("*****Test url preservation :");
			parmsFour.set(1, null); // use standard regexp
			parmsFour.set(2, 0); // no stopword elimination
			parmsFour.set(3, 1);    // want URL preservation 
			
			parmsFour.set(0, "foo");
			assertEquals("",func.exec(parmsFour).toString(), "{(foo)}");
			
			parmsFour.set(0, "http://infolab.stanford.edu");
			assertEquals("",func.exec(parmsFour).toString(), "{(http://infolab.stanford.edu)}");

			parmsFour.set(0, "And now url (embedded http://infolab.stanford.edu) text");
			assertEquals("",func.exec(parmsFour).toString(), "{(And),(now),(url),(embedded),(http://infolab.stanford.edu),(text)}");
			
			parmsFour.set(0, "The word http text.");
			assertEquals("",func.exec(parmsFour).toString(), "{(The),(word),(http),(text)}");
			
			parmsFour.set(0, "Finally, (file://C:/Users/kennedy/.baschrc) two URLs. ftp://blue.mountain.com/?parm1=foo&parm2=bar");
			assertEquals("",func.exec(parmsFour).toString(), "{(Finally),(file://C:/Users/kennedy/.baschrc),(two),(URLs),(ftp://blue.mountain.com/?parm1=foo&parm2=bar)}");
			
			//-------------
			//System.out.println("*****Now with 'fo.*o' as regexp:");
			parmsTwo.set(1, "fo.*o");
			
			parmsTwo.set(0, "foo");
			assertEquals("",func.exec(parmsTwo).toString(), "{}");
			
			parmsTwo.set(0, "fobaro");
			assertEquals("",func.exec(parmsTwo).toString(), "{}");
			
			parmsTwo.set(0, "fobarotree");
			assertEquals("",func.exec(parmsTwo).toString(), "{(tree)}");
			
			parmsTwo.set(0, "fo is your papa barotree");
			assertEquals("",func.exec(parmsTwo).toString(), "{(tree)}");
			
			parmsTwo.set(0, "fo is your papa barotree and with you.");
			assertEquals("",func.exec(parmsTwo).toString(), "{(u.)}");

			//-------------
			//System.out.println("*****Pulling out URLs:");
			
			assertEquals(RegexpTokenize.findURL("This is http://foo.bar.com/blue.html", 8), "http://foo.bar.com/blue.html");
			
			assertEquals(RegexpTokenize.findURL("file://me.you.her/blue.html", 0), "file://me.you.her/blue.html");
			
			assertEquals(RegexpTokenize.findURL("URL is ftp://me.you.her/blue.html, and embedded.", 7), "ftp://me.you.her/blue.html");
			
			assertEquals(RegexpTokenize.findURL("No index given ftp://me.you.her/blue.html, and embedded."), "ftp://me.you.her/blue.html");
			
			assertEquals(RegexpTokenize.findURL("file://me.you.her/blue.html without index"), "file://me.you.her/blue.html"); 

			System.out.println("Passed all tests.");
			
		} catch (IOException e) {

			e.printStackTrace();
		}
	}

}
