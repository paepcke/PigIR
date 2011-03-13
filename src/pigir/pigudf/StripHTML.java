package pigir.pigudf;

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
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.jsoup.Jsoup;

import pigir.Common;

 /**
  *	 Given tuple and an index to a column that contains
  *  a string, return a new tuple, which is a copy of the
  *  input, with an appended new field 'noTagContent'. This
  *  new field that contains a copy the indicated column's
  *  content with any HTML tags, and JavaScript removed.
  *   
  *  Input:
  *     columnNumber: an integer that identifies the column
  *     	 		  whose content is to be stripped of HTML.
  *     			  The integer is zero-based. 
  *     webTuple:     a tuple that contains the column to be
  *     	          processed. Other parts of the tuple are
  *     			  ignored.
  *  Output: new tuple with added field 'noTagContent'
  * 
 * @author paepcke
 *
 */
public class StripHTML extends EvalFunc<Tuple> {
	
	final int COL_PT_ARG_INDEX = 0;
	final int HTML_CONTENT_ARG_INDEX = 1;
    	
    public Tuple exec(Tuple input) throws IOException {
    	
    	Tuple tupleWithHTML = null;
    	String htmlString = null;
    	int columnIndex = -1;

    	try {
    		// Ensure presence of two parameters, and of the 
    		// first parameter being an integer >= 0:
    		if (input == null || 
    			input.size() < 2 ||
    			(columnIndex = (Integer) input.get(COL_PT_ARG_INDEX)) < 0) {
    			
    			getLogger().warn("StripHTML() encountered mal-formed input. Fewer than 2 arguments, " +
    							 "or negative column index. " +
    							 "Expecting ((int) columnIndex, (Tuple) webPageTuple)): " + input);
    			return null;
    		}
    		
    		tupleWithHTML = (Tuple) input.get(HTML_CONTENT_ARG_INDEX);
    		if (tupleWithHTML.size() <= columnIndex) {
    			// Column index points beyond the size of the tuple:
    			getLogger().warn("StripHTML(): " +
    							 columnIndex + 
    							 " is too large a column index for tuple: " +
    							 tupleWithHTML + ".");
    			return null;
    		}
    		// Get a copy of the HTML string:
    		htmlString = (String) tupleWithHTML.get(columnIndex);

    	} catch (ClassCastException e) {
    		getLogger().warn("StripHTML() encountered mal-formed input; expecting ((int) columnIndex, (Tuple) webPageTuple)): " + input);
    		return null;
    	}
    
    	String strippedStr = extractText(htmlString);
    	tupleWithHTML.append(strippedStr);
    	return tupleWithHTML;
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
        try{
        	//****************
        	System.out.println("stripHTML input schema:" + inputSchema);
        	Schema resSchema = new Schema();
        	resSchema.add(inputSchema.getField(1));
        	return resSchema;
        	//****************
        	/* *****************
        	Schema resSchema = new Schema();
        	// We don't have a definitive name (alias) for this result tuple,
        	// therefore the first null:
        	resSchema.add(new Schema.FieldSchema("secondInputArgPlusStrippedTextField", DataType.TUPLE));
            return resSchema;
            ***************** */
        }catch (Exception e){
                return null;
        }
    }    

	/* *************
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FieldSchema> fields = new ArrayList<FieldSchema>(2);
        fields.add(new FieldSchema("columnNumber", DataType.INTEGER));
        fields.add(new FieldSchema("webTuple", DataType.TUPLE));
        FuncSpec funcSpec = new FuncSpec(this.getClass().getName(), new Schema(fields));
        List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>(1);
        funcSpecs.add(funcSpec);
        return funcSpecs;
    }	
    **************/
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
    	List<FuncSpec> funcList = new ArrayList<FuncSpec> ();
    	// Schema portion for first argument (that being an integer):
    	Schema.FieldSchema columnIndexFld = new Schema.FieldSchema(null, DataType.INTEGER);
    	Schema.FieldSchema webPageTupleFld = new Schema.FieldSchema(null, DataType.TUPLE);
    	// Pack these into a schema:
    	Schema colSpecIsInteger = new Schema();
    	colSpecIsInteger.add(columnIndexFld);
    	colSpecIsInteger.add(webPageTupleFld);
    	funcList.add(new FuncSpec(this.getClass().getName(), colSpecIsInteger));
    	return funcList; 
    }	
    
	private String extractText(Reader reader) throws IOException {
		StringBuilder sb = new StringBuilder();
		BufferedReader br = new BufferedReader(reader);
		String line;
		while ( (line=br.readLine()) != null) {
			sb.append(line);
		}
		String textOnly = Jsoup.parse(sb.toString()).text();
		return textOnly;
	}
	
	private String extractText(String webPage) throws IOException {
		return extractText(new StringReader(webPage));
	}
    /* ---------------------------------   T E S T I N G ------------------------------*/

    private boolean doTests() {
    	
    	String htmlStr = "<head><html>This is <b>bold</b> and a <a href='http://test.com'>link anchor</a></html></head>";
    	Tuple webPage = TupleFactory.getInstance().newTuple(htmlStr);
    	int colIndexToHTMLContent = 0;
    	Tuple input = TupleFactory.getInstance().newTuple(2);
    	Tuple res;

    	try {
    		input.set(0, colIndexToHTMLContent);
    		input.set(1, webPage);
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
