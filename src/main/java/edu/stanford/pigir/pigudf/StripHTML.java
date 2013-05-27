package edu.stanford.pigir.pigudf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.safety.Whitelist;

 /**
  *	 Given tuple whose first element contains
  *  a string, return a new tuple with two fields.
  *  The first is the string stripped of all HTML tags
  *  and JavaScript. The second is that string's length.
  *   
  *  Input:
  *     webTuple:     a tuple that contains the html text to be
  *     	          processed. Other parts of the tuple are
  *     			  ignored.
  *  Output: new tuple: (strippedStr:chararray, strLen:int);
  * 
 * @author paepcke
 *
 */
public class StripHTML extends EvalFunc<Tuple> {
	
	final int RESULT_CONTENT_POSITION = 0;
	final int RESULT_CONTENT_LENGTH_POSITION = 1;
	final int COL_PT_ARG_INDEX = 0;
	final int HTML_CONTENT_ARG_INDEX = 1;
    	
    public Tuple exec(Tuple input) throws IOException {
    	
    	String htmlString = null;
    	
    	try {
    		// Ensure presence of one parameter (the string):
    		if (input == null || 
    			input.size() < 1) {
    			getLogger().warn("StripHTML() encountered mal-formed input. Fewer than 1 argument. " +
    							 "Expecting HTML string, but called with: " + input);
    			return null;
    		}
    		
    		if (input.get(0) instanceof DataByteArray)
    			htmlString = ((DataByteArray)input.get(0)).toString();
    		else if (input.get(0) instanceof String)
    			htmlString = (String) input.get(0);
    		else {
    			getLogger().warn("StripHTML() encountered mal-formed input. Argument is not " +
    							 "String or bytearray. Input is: " + input);
    			return null;
    		}
    	} catch (ClassCastException e) {
    		getLogger().warn("StripHTML() encountered mal-formed input; expecting a string, but called with: " + 
    					input);
    		return null;
    	}
    	Tuple res = TupleFactory.getInstance().newTuple(2);
    	String strippedContent = extractText(htmlString);
    	
    	if (input.get(0) instanceof DataByteArray)
    		res.set(RESULT_CONTENT_POSITION, new DataByteArray(strippedContent.getBytes()));
    	else 
    		// Content to strip was passed in as a String:
    		res.set(RESULT_CONTENT_POSITION, strippedContent);
    	
    	res.set(RESULT_CONTENT_LENGTH_POSITION, strippedContent.length());
    	return res;
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
        public Schema outputSchema(Schema input) {
        try{
            Schema tupleSchema = new Schema();
            tupleSchema.add(input.getField(0));
            tupleSchema.add(new Schema.FieldSchema("content-length", DataType.INTEGER));
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
                                                                              tupleSchema, DataType.TUPLE));
        }catch (Exception e){
                return null;
        }
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
    
	@SuppressWarnings("unused")
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
		//******String cleanText = Jsoup.clean(webPage, Whitelist.none());

	    Document doc = Jsoup.parse( webPage );
	    doc.outputSettings().charset("UTF-8");
	    String htmlText = Jsoup.clean( doc.body().html(), Whitelist.none() );
	    htmlText = StringEscapeUtils.unescapeHtml(htmlText);
	    return htmlText;		
		
/*		// Parse str into a Document
		Document doc = Jsoup.parse(webPage);
		// Clean the document.
		doc = new Cleaner(Whitelist.simpleText()).clean(doc);
		// Adjust escape mode
		doc.outputSettings().escapeMode(EscapeMode.xhtml);
		// Get back the string of the body.
		String cleanText = doc.body().html();		
		return cleanText;
*/	}
}
