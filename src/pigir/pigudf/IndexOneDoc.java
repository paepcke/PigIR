package pigir.pigudf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;


/**
 * Compute index entries for the words in a given document.
 * 
 * <b>Input:</b><br>  
 * <ul>
 * <li> A document ID.
 * <li> Document string (usually HTML tag-free) 
 * <li> Stopword elimination <optional>. If present, null to keep default, 
 * 			0 to turn feature off, or 1 to turn it on (default is 1)
 * <li> URL preservation <optional>. If present, null to keep default, 
 * 			0 to turn feature off, or 1 to turn it on (default is 1) 
 * <li> Split regexp <optional>. If present, null to keep default, or a regular 
 * 			expression string acceptable to Java split(). For default, 
 * 			@see pigir.pigudf.RegexpTokenizer#defaultSepRegexp RegexpTokenizer.
 * </ul>

 * <b>Output:</b><br>  
 * Bag of tuples structured like this: {@code (word,docid,wordPosition)}
 *
 * Usage scenario: indexing a Web site in a Map/Reduce context. 
 * This method is a mapper.
 * 
 * @see RegexpTokenize
 * 
 * @author paepcke
 */

public class IndexOneDoc extends EvalFunc<DataBag> {
	
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();
    String docID = null;
    String docContent = null;
    
	// Index of start of the most recently found URL wbRecordReader str.
	private int urlIndex = 0;
	// For skipping past URL components after they have been
	// parsed separately:
	private int tokensToSkip = 0;
	private int numSkippedTokens = 0;
	
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
    
    final String usage = "Expecting IndexOneDoc(String docID, " +
    		"String content, [Integer stopwordElim [,Integer preserveURL [,String regexp]]])";

    public DataBag exec(Tuple input) throws IOException {
    	
    	DataBag output = mBagFactory.newDefaultBag();
    	Tuple onePosting = null;
    	String splitRegexp = RegexpTokenize.getDefaultRegexp();
    	Boolean doStopwordEliminiation = true;
    	Boolean preserveURLs = true;

    	try {
    	if (input == null || 
    		input.size() < 2 || 
    		(docID = (String) input.get(0)) == null ||
    		(docContent = (String) input.get(1)) == null)
    		return null;
    	} catch (ClassCastException e) {
    		throw new IOException("IndexOneDoc: incorrect number, or type of parameters. " +
    				usage + ". Called with " + input);
    				
    	} catch (Exception e) {
    		throw new IOException(usage + ". Experienced error: " + e.getMessage());
    	}
    	
    	// Any of the optional parameters present?
    	Object parm = null; 
        try {
			// If a third arg is given, it's null for default, 0 for no stopword elimination,
        	// or 1 to eliminate stopwords: 
			if (input.size() > 2) {
				if (((parm = input.get(2)) != null) &&
					((Integer) parm == 0)) {
					doStopwordEliminiation = false;
				}
			}
			// If a fourth arg is given, it's null for default, 0, or 1 to indicate
			// whether we should preserve URLs:
			if (input.size() > 3) {
				if (((parm = input.get(3)) != null) &&
					((Integer) parm == 0)) {
					preserveURLs = false;
				}
			}
			
			// If a fifth arg is given, it's null  for default,
			// a string that is the regular expression to use for tokenizing:
			if (input.size() > 4) {
				if ((parm = input.get(4)) != null) {
					splitRegexp = (String) parm;
				}
			}
    	} catch (ClassCastException e) {
    		throw new IOException("IndexOneDoc(): Wrong type of parameter. " +
    				usage + ". Got: " + input);
        } catch (Exception e) {
    		throw new IOException("IndexOneDoc: incorrect number, or type of parameters. " +
    				usage + ". Called with " + input);
        }
        
        // Prepare output bag:
        output = mBagFactory.newDefaultBag();
        String[] resArray = docContent.split(splitRegexp);
        String token;
        urlIndex = 0;
        tokensToSkip = 0;
        numSkippedTokens = 0;
        
        for (int tokenPos=0; tokenPos<resArray.length; tokenPos++) {
      	
        	// If we are skipping over a URL:
        	if (tokensToSkip > 0) {
        		tokensToSkip--;
        		numSkippedTokens++;
        		continue;
        	}
        	
        	token = resArray[tokenPos];
        	
        	// Substrings that are themselves separators show up as
        	// empty strings. Don't make an empty tuple for those:
        	if (token.isEmpty())
        		continue;
        	if (doStopwordEliminiation)
        		if (IsStopword.isStopword(token))
        			continue;
        	if (preserveURLs && (webProtocols.get(token) != null)) {
        		// Found one of the Web protocol names (http, ftp, file, ...).
        		// We need to find that URL string and return it intact.
        		// Find the URL, starting the search where we last found a 
        		// URL (or 0 for the first URL). The urlIndex is advanced
        		// below, but we only ever point to right after the previous
        		// token. So to point to the start of the URL we need
        		// to advance the str pointer:
        		urlIndex = ((String)docContent).indexOf(token, urlIndex);
        		token = RegexpTokenize.findURL((String)docContent, urlIndex);
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
        	onePosting = mTupleFactory.newTuple();
        	onePosting.append(token);
        	onePosting.append(docID);
        	
        	onePosting.append(tokenPos - numSkippedTokens);
        	output.add(onePosting);
        }
        return output;
    }
    
	public Schema outputSchema(Schema input) {
        try{
            Schema postingsSchema = new Schema();
        	postingsSchema.add(new Schema.FieldSchema("word", DataType.CHARARRAY));
        	postingsSchema.add(new Schema.FieldSchema("docID", DataType.CHARARRAY));
        	postingsSchema.add(new Schema.FieldSchema("tokenPos", DataType.INTEGER));
        	
        	Schema bagOfPostingsSchema = new Schema();
        	bagOfPostingsSchema.add(new Schema.FieldSchema("postingsInDoc", postingsSchema, DataType.BAG));
        	
            return bagOfPostingsSchema;
        }catch (Exception e){
                return null;
        }
    }    
	
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
    	// Just one exec function handles all parameter combinations: 
        List<FieldSchema> fields = new ArrayList<FieldSchema>(1);
        fields.add(new FieldSchema(null, DataType.BAG));
        FuncSpec funcSpec = new FuncSpec(this.getClass().getName(), new Schema(fields));
        List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>(1);
        funcSpecs.add(funcSpec);
        return funcSpecs;
    }	
}
