package pigir.pigudf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Given an HTML string, return a tuple of strings that comprise
 * the anchor texts in the HTML.
 * 
 * @author paepcke
 *
 */
public class AnchorText extends EvalFunc<Tuple> {
	
	int MIN_LINK_LENGTH = "<a href=\"\"></a>".length();
    TupleFactory mTupleFactory = TupleFactory.getInstance();
	public final Logger logger = Logger.getLogger(getClass().getName());
	
	// The '?' after the .* before the </a> turns this 
	// match non-greedy. Without the question mark, the
	// .* would eat all the html to the last </a>:
	private Pattern pattern = Pattern.compile("<a[\\s]+href[\\s]*=[\\s]*\"[^>]*>(.*?)</a>");
    
    public Tuple exec(Tuple input) throws IOException {
    	
    	String html = null;
    	Tuple output = mTupleFactory.newTuple();
		
    	try {
    		if ((input.size() == 0) || 
    				((html = (String) input.get(0)) == null) ||
    				(html.length() < MIN_LINK_LENGTH)) {
    			return null;
    		}
    	} catch (ClassCastException e) {
    		throw new IOException("AnchorText(): bad input: " + input);
    	}
		Matcher matcher = pattern.matcher(html);
		while(matcher.find()){
			output.append(matcher.group(1));
		}
		return output;
    }
    
	public Schema outputSchema(Schema input) {
        try{
            Schema anchorTextSchema = new Schema();
        	anchorTextSchema.add(new Schema.FieldSchema("text", DataType.CHARARRAY));
        	
        	// Schema of all anchor text strings:
        	Schema outSchema = new Schema(new Schema.FieldSchema("anchorTexts", anchorTextSchema, DataType.TUPLE));
            return outSchema;
        }catch (Exception e){
                return null;
        }
    }
	
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
    	
        List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>();
        
        // Call with one parameter: an HTML string:
        List<FieldSchema> htmlFieldSchema = new ArrayList<FieldSchema>(1);
        htmlFieldSchema.add(new FieldSchema(null, DataType.CHARARRAY));  // the HTML document
        FuncSpec htmlParameterOnly = new FuncSpec(this.getClass().getName(), new Schema(htmlFieldSchema));
        funcSpecs.add(htmlParameterOnly);
        
        return funcSpecs;
    }
}
