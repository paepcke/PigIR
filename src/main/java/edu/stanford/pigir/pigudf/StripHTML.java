package edu.stanford.pigir.pigudf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

import edu.stanford.pigir.Common;

 /**
  *	 Given tuple whose first element contains
  *  a string, return a new tuple, which is a copy of the
  *  input, with an appended new field 'noTagContent'. This
  *  new field contains a copy the first column's content,
  *  with any HTML tags, and JavaScript removed.
  *   
  *  Input:
  *     webTuple:     a tuple that contains the html text to be
  *     	          processed. Other parts of the tuple are
  *     			  ignored.
  *  Output: new tuple with added field 'noTagContent'
  * 
 * @author paepcke
 *
 */
public class StripHTML extends EvalFunc<String> {
	
	final int COL_PT_ARG_INDEX = 0;
	final int HTML_CONTENT_ARG_INDEX = 1;
    	
    public String exec(Tuple input) throws IOException {
    	
    	String htmlString = null;
    	
    	try {
    		// Ensure presence of one parameter (the string):
    		if (input == null || 
    			input.size() < 1) {
    			getLogger().warn("StripHTML() encountered mal-formed input. Fewer than 2 arguments. " +
    							 "Expecting HTML string, but called with: " + input);
    			return null;
    		}
    		
    		htmlString = (String) input.get(0);

    	} catch (ClassCastException e) {
    		getLogger().warn("StripHTML() encountered mal-formed input; expecting a string, but called with: " + 
    					input);
    		return null;
    	}
    	return extractText(htmlString);
    }
    
	/* (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#outputSchema(org.apache.pig.impl.logicalLayer.schema.Schema)
	 * The input schema is just the column index and the tuple T that contains the
	 * Web page data. We return T with the string appended.
	 * 
	 * Note: we could grab the result schema from the second field of the
	 *       input schema. We would do that if that schema were a nested
	 *       schema that defines the tuple (the one with the HTML in it).
	 *       But that is just defined as Tuple. It would be like this:
	 *       resSchema.add(inputSchema.getField(HTML_CONTENT_ARG_INDEX));
	 */
    
	public Schema outputSchema(Schema inputSchema) {
		// We return a string, just as we were given:
		return inputSchema;
    }    
	
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
    	List<FuncSpec> funcList = new ArrayList<FuncSpec> ();
    	// Schema portion for first argument (that being an integer):
    	Schema.FieldSchema webPageTupleFld = new Schema.FieldSchema(null, DataType.CHARARRAY);
    	// Pack this parameter into a schema:
    	Schema inputSchema = new Schema();
    	inputSchema.add(webPageTupleFld);
    	funcList.add(new FuncSpec(this.getClass().getName(), inputSchema));
    	return funcList; 
    }	
    
	private String extractText(Reader reader) throws IOException {
		StringBuilder sb = new StringBuilder();
		BufferedReader br = new BufferedReader(reader);
		String line;
		while ( (line=br.readLine()) != null) {
			sb.append(line);
		}
		String textOnly = Jsoup.clean(sb.toString(), Whitelist.none());
		return textOnly;
	}
	
	private String extractText(String webPage) throws IOException {
		return extractText(new StringReader(webPage));
	}
    /* ---------------------------------   T E S T I N G ------------------------------*/

    private boolean doTests() {
    	
    	String htmlStr = "<head><html>This is <b>bold</b> and a <a href='http://test.com'>link anchor</a></html></head>";
    	//Tuple webPage = TupleFactory.getInstance().newTuple(htmlStr);
    	//Tuple input = TupleFactory.getInstance().newTuple(1);
    	Tuple input = TupleFactory.getInstance().newTuple(htmlStr);
    	String res;

    	try {
    		res = exec(input);
    		System.out.println(res);
    		System.out.println(outputSchema(Common.getTupleSchema(input)));
    		System.out.println(getArgToFuncMapping());
    	} catch (Exception e) {
    		System.out.println(e.getMessage());
    		return false;
    	}
    	System.out.println("All Good.");
    	return true;
    }
    
    public static void main(String[] args) {
    	new StripHTML().doTests();
    };
}
