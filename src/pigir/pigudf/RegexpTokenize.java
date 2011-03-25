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
 * arg2 <optional>: null to use default, or a regular expression acceptable to Java split();
 * arg3 <optional>: null to use default; 1 to eliminate stopwords, 0 for no stopword elimination. (default is 1)
 * arg4 <optional>: null to use default; 1 to treat URLs as one token, 0 to split URLs into multiple tokens. (default is 1)
 * 
 * @author paepcke
 *
 */

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
    	Boolean doStopwordEliminiation = true;
    	Boolean preserveURLs = true;
    	Integer stopWordParm = null;
    	Integer urlPreserveParm = null;
    	
    	Object str = null;
    	
    	if (input == null || input.size() == 0 || (str = input.get(0)) == null) 
    		return null;

    	urlIndex = 0;
    	tokensToSkip = 0;
        try {
			// If a second arg is given, it's a regexp for how to split:
			if (input.size() > 1) {
				if ((splitRegexp = (String) input.get(1)) == null)
					splitRegexp = defaultSepRegexp;
			}
			// If a third arg is given, it's null or 1 to indicate
			// whether stopword elimination is wanted:
			if (input.size() > 2)
				if ((stopWordParm = (Integer) input.get(2)) != null) {
					if (stopWordParm == 0)
						doStopwordEliminiation = false;
					else
						doStopwordEliminiation = true;
				}
			
			// If a fourth arg is given, it's null or 1 to indicate
			// whether we should preserve URLs:
			if (input.size() > 3)
				if ((urlPreserveParm = (Integer) input.get(3)) != null) {
					if (urlPreserveParm == 0)
						preserveURLs = false;
					else
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
            } // end for
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
    	final Matcher urlMatcher = urlPattern.matcher("");
    	urlMatcher.reset(str);
    	
    	if (urlMatcher.find(startIndex)) {
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
}
