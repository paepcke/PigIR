package pigir.pigudf;

import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import pigir.Common;

/**
 * Extension of standard tokenizer. This UDF adds the ability to
 * provide a regular expression that is to be used for splitting
 * strings into tokens. The UDF also provides the option to eliminate
 * stopwords, and to preserve URLs through the tokenization process.
 * 
 * The list of stopwords is encapsulated in the IsStopword() UDF.
 * 
 * Arguments:
 * arg1: String to be tokenized
 * arg2 <optional>: if present, null, or a regular expression acceptable to Java split();
 * arg3 <optional>: if present, null, or 1 controls whether stopwords are to be eliminated.
 * arg4 <optional>: if present, null, or 1 controls whether URLs are to preserved. 
 * 
 * @author paepcke
 *
 */
/* TODO: convert test sequence in commented Main to auto test. */

public class RegexpTokenize extends EvalFunc<DataBag> {

	public static final String USE_DEFAULT_SPLIT_REGEXP = Common.PIG_FALSE;
	public static final String PRESERVE_URLS = Common.PIG_TRUE;
	public static final String SPLIT_URLS = Common.PIG_FALSE;
	public static final String KILL_STOPWORDS = Common.PIG_TRUE;
	public static final String PRESERVE_STOPWORDS = Common.PIG_FALSE;
	
		
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();
    
    // The \u00a0 is the Unicode non-breaking space. It is sometimes used
    // by Web designers to space words. 
    private static final String defaultSepRegexp = "[\\s!\"#$%&/'()*+,-.:;<=>?@\\[\\]^_`{|}\u00a0]";
    
    private static final String urlSlurpRegexp   = "([:/\\p{Alnum}.\\-*_+%?&=~]*).*";
    
	@SuppressWarnings("serial")
	private static final HashMap<String, Boolean> webProtocols = new HashMap<String,Boolean>() {
		{
			put("http", true);
			put("https", true);
			put("ftp", true);
			put("ftps", true);
			put("file", true);
			put("mailto", true);
			put("rtsp", true);
		};
	};

	// Index of start of the most recently found URL wbRecordReader str.
	private int urlIndex = 0;
	// For skipping past URL components after they have been
	// parsed separately:
	private int tokensToSkip = 0;

	
    public DataBag exec(Tuple input) throws IOException {
    	DataBag output = null;
    	String splitRegexp = defaultSepRegexp;
    	Boolean doStopwordEliminiation = false;
    	Boolean preserveURLs = false;
    	
    	Object str = null;
    	
    	if (input == null || input.size() == 0 || (str = input.get(0)) == null) 
    		return null;

    	urlIndex = 0;
    	tokensToSkip = 0;
        try {
			// If a second arg is given, it's a regexp for how to split:
			if (input.size() > 1) {
				if ((splitRegexp = (String) input.get(1)) == null) {
					splitRegexp = defaultSepRegexp;
				}
			}
			// If a third arg is given, it's null or 1 to indicate
			// whether stopword elimination is wanted:
			if (input.size() > 2)
				if (input.get(2) != null) {
					doStopwordEliminiation = true;
				}
			
			// If a fourth arg is given, it's null or 1 to indicate
			// whether we should preserve URLs:
			if (input.size() > 3)
				if (input.get(3) != null) {
					preserveURLs = true;
				}
			
			// Prepare output bag:
            output = mBagFactory.newDefaultBag();
            if (!(str instanceof String)) {
                throw new IOException("Expected input to be chararray, but  got " + str.getClass().getName());
            }
            String[] resArray = ((String)str).split(splitRegexp);
            urlIndex = 0;
            tokensToSkip = 0;
            
            for (String token : resArray) {
            	// If we are skipping over a URL:
            	if (tokensToSkip > 0) {
            		tokensToSkip--;
            		continue;
            	}
            	// Substrings that are themselves separators show up as
            	// empty strings. Don't make an empty tuple for those:
            	if (token.isEmpty())
            		continue;
            	if (doStopwordEliminiation)
            		if (IsStopword.isStopword(token))
            			continue;
            	if (preserveURLs && (webProtocols.get(token) != null)) {
            		// Found one of the Web protocol names (http, ftp, file, ...).
            		// We need to find that URL wbRecordReader the string and return it intact.
            		// Find the URL, starting the search where we last found a 
            		// URL (or 0 for the first URL). The urlIndex is advanced
            		// below, but we only ever point to right after the previous
            		// token. So to point to the start of the URL we need
            		// to advance the str pointer:
            		urlIndex = ((String)str).indexOf(token, urlIndex);
            		token = findURL((String)str, urlIndex);
            		// We'll need to skip over the next few tokens. They
            		// were fragmented before we realized that we were
            		// looking at a URL. Split the URL the way we
            		// erroneously split it, and thereby find the number of 
            		// tokens to skip:
            		String [] urlFrags = token.split(splitRegexp);
            		tokensToSkip = urlFrags.length - 1;
            		// Point past the URL, so that 
            		// we'll start searching for URLs *after* this one that
            		// we just found next time we find a URL:
            		urlIndex += token.length();
            	}
            	output.add(mTupleFactory.newTuple(token)); 
            	// If we are to preserve URLs, we need to keep track
            	// of where we are wbRecordReader the original string:
            }
        } catch (ExecException ee) {
            throw new IOException("Regexp tokenizing failed.", ee);
        }
		return output;
    }

	public Schema outputSchema(Schema input) {
        try{
        	Schema oneWordTupleSchema = new Schema();
           	oneWordTupleSchema.add(new Schema.FieldSchema("token", DataType.CHARARRAY));
            Schema outSchema = new Schema(new Schema.FieldSchema("tokenSet",
            					                                 oneWordTupleSchema, 
            					                                 DataType.BAG));
            return outSchema;

        }catch (Exception e){
                return null;
        }
    }
    
    public static String findURL(String str, int startIndex) {
    	
    	final Pattern urlPattern = Pattern.compile(urlSlurpRegexp);
    	Matcher urlMatcher = urlPattern.matcher(str.substring(startIndex));
    	
    	if (urlMatcher.matches()) {
    		return urlMatcher.group(1);
    	} else return null;
    }
    
    public static String findURL(String str) {
    	
    	int startIndex = -1;
    	for (String webProto : webProtocols.keySet()) {
    		if ((startIndex = str.indexOf(webProto)) != -1)
    			return findURL(str, startIndex);
    	}
    	return null;
    }
    
    public static String getDefaultRegexp() {
    	return defaultSepRegexp;
    }

    // ------------------------------------  Testing -------------------------

/*
	public static void main(String[] args) {
		
		RegexpTokenize func = new RegexpTokenize();
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		Tuple parmsTwo = tupleFac.newTuple(2);
		Tuple parmsThree = tupleFac.newTuple(3);
		Tuple parmsFour = tupleFac.newTuple(4);
		
		try {
			System.out.println("*****With default regexp:");
			
			parms.set(0, "On a sunny day");
			System.out.println("'On a sunny day': " + func.exec(parms));
			
			parms.set(0, "Testing it!");
			System.out.println("'Testing it!': " + func.exec(parms));
			
			parms.set(0, "FDA");
			System.out.println("'FDA': " + func.exec(parms));
			
			//-------------
			System.out.println("*****Now with whitespace as regexp:");
			parmsTwo.set(1, "[\\s]");

			parmsTwo.set(0, "On a sunny day");
			System.out.println("'On a sunny day': " + func.exec(parmsTwo));
			
			parmsTwo.set(0, "Testing it!");
			System.out.println("'Testing it!': " + func.exec(parmsTwo));
			
			parmsTwo.set(0, "FDA");
			System.out.println("'FDA': " + func.exec(parmsTwo));

			//-------------
			System.out.println("*****Test stopword elimination:");
			parmsThree.set(1, null);
			parmsThree.set(2, 1);
			
			parmsThree.set(0, "foo");
			System.out.println("'foo': " + func.exec(parmsThree));
			
			parmsThree.set(0, "This is a stopword test.");
			System.out.println("'This is a stopword test.': " + func.exec(parmsThree));
			//-------------
			System.out.println("*****Test url preservation :");
			parmsFour.set(1, null); // use standard regexp
			parmsFour.set(2, null); // no stopword elimination
			parmsFour.set(3, 1);    // want URL preservation 
			
			parmsFour.set(0, "foo");
			System.out.println("'foo': " + func.exec(parmsFour));
			
			parmsFour.set(0, "http://infolab.stanford.edu");
			System.out.println("'http://infolab.stanford.edu': " + func.exec(parmsFour));

			parmsFour.set(0, "And now url (embedded http://infolab.stanford.edu) wbRecordReader text");
			System.out.println("'And now url (embedded http://infolab.stanford.edu) wbRecordReader text': " + func.exec(parmsFour));
			
			parmsFour.set(0, "The word http wbRecordReader text.");
			System.out.println("'The word http wbRecordReader text.': " + func.exec(parmsFour));
			
			parmsFour.set(0, "Finally, (file://C:/Users/kennedy/.baschrc) two URLs. ftp://blue.mountain.com/?parm1=foo&parm2=bar");
			System.out.println("'Finally, (file://C:/Users/kennedy/.baschrc) two URLs. ftp://blue.mountain.com/?parm1=foo&parm2=bar': " + func.exec(parmsFour));
			
			
			//-------------
			System.out.println("*****Now with 'fo.*o' as regexp:");
			parmsTwo.set(1, "fo.*o");
			
			parmsTwo.set(0, "foo");
			System.out.println("'foo': " + func.exec(parmsTwo));
			
			parmsTwo.set(0, "fobaro");
			System.out.println("'fobaro': " + func.exec(parmsTwo));
			
			parmsTwo.set(0, "fobarotree");
			System.out.println("'fobarotree': " + func.exec(parmsTwo));
			
			parmsTwo.set(0, "fo is your papa barotree");
			System.out.println("'fo is your papa barotree': " + func.exec(parmsTwo));
			
			parmsTwo.set(0, "fo is your papa barotree and with you.");
			System.out.println("'fo is your papa barotree and with you.': " + func.exec(parmsTwo));

			//-------------
			System.out.println("*****Pulling out URLs:");
			
			System.out.println("'This is http://foo.bar.com/blue.html': '" + 
						func.findURL("This is http://foo.bar.com/blue.html", 8) +
						"'");
			
			System.out.println("'file://me.you.her/blue.html': '" + 
						func.findURL("file://me.you.her/blue.html", 0) +
						"'");
			
			System.out.println("'URL is ftp://me.you.her/blue.html, and embedded.': '" + 
						func.findURL("URL is ftp://me.you.her/blue.html, and embedded.", 7) +
						"'");
			
			System.out.println("'No index given ftp://me.you.her/blue.html, and embedded.': '" + 
						func.findURL("No index given ftp://me.you.her/blue.html, and embedded.") +
						"'");
			
			System.out.println("'file://me.you.her/blue.html without index': '" + 
						func.findURL("file://me.you.her/blue.html without index") +
						"'");

			
		} catch (IOException e) {

			e.printStackTrace();
		}
	}
*/
}
