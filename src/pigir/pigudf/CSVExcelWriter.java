package pigir.pigudf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


public class CSVExcelWriter extends PigStorage implements StoreFuncInterface {

	final static int COMMA = ',';
	final static int DOUBLE_QUOTE = '\"';
	final static int LINEFEED = '\n';
	final static int NEWLINE = '\r';
	
	final Logger logger = Logger.getLogger(getClass().getName());
	
	// For Excel to properly read multiple lines within a 
	// field, the line break must be CRLF (\r\n, a.k.a. ^M^J), where
	// in Unix it's just LF (\n a.k.a. ^J). We need to
	// ensure this convention is met when writing field-embedded
	// multi-lines. Compile pattern, and prepare a re-usable
	// matcher object for speed. We search for, and capture in 
	// a group, any substring that ends in a lone \n. The group
	// is needed for inserting the \r later:
	Pattern loneCRDetectorPattern = Pattern.compile("([^\r])\n", Pattern.DOTALL | Pattern.MULTILINE);
	Matcher loneCRDetector = loneCRDetectorPattern.matcher("");

	
	// Pig Storage with COMMA as delimiter:
	TupleFactory tupleMaker = TupleFactory.getInstance();
	
	public CSVExcelWriter() {
		super(",");
	}
	
    @Override
    public void putNext(Tuple tupleToWrite) throws IOException {
    	ArrayList<Object> mProtoTuple = new ArrayList<Object>();
    	int embeddedNewlineIndex = -1;
    	String fieldStr = null;
    	
    	// Do the escaping:
    	for (Object field : tupleToWrite.getAll()) {
    		fieldStr = field.toString();
    		// Embedded double quotes are replaced by two double quotes:
    		fieldStr = fieldStr.replaceAll("[\"]", "\"\"");
    		// If any commas are in the field, or if we did replace
    		// any double quotes with a pair of double quotes above,
    		// or if the string includes a newline character (\n:0x0A),
    		// then the entire field must be enclosed in double quotes:
    		embeddedNewlineIndex =  fieldStr.indexOf(LINEFEED);
    		if ((fieldStr.indexOf(COMMA) != -1) || 
    			(fieldStr.indexOf(DOUBLE_QUOTE) != -1) ||
    			(embeddedNewlineIndex != -1))  {
    			fieldStr = "\"" + fieldStr + "\"";
    		}
    		
    		// Replace any Linefeed-only (^J), with LF-Newline (^M^J),
    		// This is needed for Excel to recognize a field-internal 
    		// new line:
/******************    		
    		if (embeddedNewlineIndex != -1) {
    			loneCRDetector.reset(fieldStr);
    			loneCRDetector.matches();
    			fieldStr = loneCRDetector.replaceAll("$1\r\n");
    		}
**************/    		
    		mProtoTuple.add(fieldStr);    		
    	}
    	// Append a newline (0x0D a.k.a. ^M to the last field
    	// so that the row termination will end up being
    	// \r\n, once the superclass' putNext() method
    	// is done below:
/* **************    	
    	if (fieldStr != null)
    		mProtoTuple.set(mProtoTuple.size() - 1, fieldStr + "\r"); 
*********/    	
    	Tuple resTuple = tupleMaker.newTuple(mProtoTuple);
    	// Checking first for debug enabled is faster:
    	if (logger.isDebugEnabled())
    		logger.debug("Row: " + resTuple);
    	super.putNext(resTuple);
    }
    
    //------------------------- Testing ------------------------

    /*
    @SuppressWarnings("serial")
	public static void main(String[] args) {
    	CSVExcelWriter writer = new CSVExcelWriter();
    	ArrayList<Object> fieldList = null;
    	
    	fieldList = new ArrayList<Object>() {
    		{
    			add(1);
    			add(2);
    			add(3);
    		}
    	};
    	//System.out.println(writer.putNext(TupleFactory.getInstance().newTuple(fieldList)));
    }
    */
}
