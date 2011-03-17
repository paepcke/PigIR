/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pigir.pigudf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.StorageUtil;
import org.apache.pig.impl.util.UDFContext;

/**
 * CSV loading and storing, with special attention to Excel 2007.
 * Arguments allow for control over:
 * 
 *    o Which delimiter is used (default is ',')
 *    o Whether line breaks are allowed inside of fields
 *    o Whether line breaks are to be written Unix style, of Windows style
 * 
 * Usage: STORE x INTO '<destFileName>' 
 *                USING CSVExcelStorage(['<del>' [,{'YES_MULTILINE' | 'NO_MULTILINE'} [,{'UNIX' | 'WINDOWS' | 'UNCHANGED'}]]]);

 *        Defaults are comma, 'NO_MULTILINE', 'UNCHANGED'
 *        The linebreak parameter is only used during store. During load
 *        no conversion is performed.                
 *                
 * Example: STORE res INTO '/tmp/result.csv' 
 *			      USING CSVExcelStorage(',', 'NO_MULTILINE', 'WINDOWS');
 *			
 *			would expect to see comma separated files for load, would
 *			use comma as field separator during store, and would treat
 *			every newline as a record terminator, and would use NL-LF
 *          as line break characters (0x0d 0x0a: \r\n).
 *          
 * Example: STORE res INTO '/tmp/result.csv' 
 *			      USING CSVExcelStorage(',', 'YES_MULTILINE');
 *
 *	        would allow newlines inside of fields. During load
 *	 	    such fields are expected to conform to the Excel
 *			requirement that the field is enclosed in double quotes.
 *			On store, the chararray containing the field will be
 *			enclosed in double quotes.
 *
 * Note: a danger with enabling multiline fields during load is that unbalanced
 * 		 double quotes will cause slurping up of input until a balancing double
 * 		 quote is found, or until something breaks. If you are not expecting
 * 		 newlines within fields it is therefore more robust to use NO_MULTILINE,
 * 	     which is the default for that reason.
 * 
 * Excel expects double quotes within fields to be escaped with a second
 * double quote. When such an embedding of double quotes is used, Excel
 * additionally expects the entire fields to be surrounded by double quotes.
 *
 * Tested with: Pig 0.8.0, Windows Vista, Excel 2007 SP2 MSO(12.0.6545.5004)
 * 
 * Known Issues: 
 * 
 * 		o When using TAB ('\t') as the field delimiter, Excel does not 
 * 	      properly handle newlines embedded in fields. Maybe there is a trick,
 * 	      but I have not found it.
 *      o Excel will only deal properly with embedded newlines if the file
 *        name has a .csv extension. 
 * 
 * 
 * @author paepcke. The load portion is based on Dmitriy V. Ryaboy's CSVLoader,
 * 					which in turn is loosely based on a version by James Kebinger.
 *
 */

public class CSVExcelStorage extends PigStorage implements StoreFuncInterface, LoadPushDown {

	public static enum Linebreaks {UNIX, WINDOWS, NOCHANGE};
	public static enum Multiline {YES, NO};
	
	protected final static byte LINEFEED = '\n';
	protected final static byte NEWLINE = '\r';
    protected final static byte DOUBLE_QUOTE = '"';
	protected final static byte RECORD_DEL = LINEFEED;
	
	private static byte FIELD_DEL = ',';
	private static String MULTILINE_DEFAULT_STR = "NOMULTILINE";
	private static String LINEBREAKS_DEFAULT_STR = "NOCHANGE";
	private static Multiline MULTILINE_DEFAULT = Multiline.NO;
	private static Linebreaks LINEBREAKS_DEFAULT = Linebreaks.NOCHANGE;
    
	long end = Long.MAX_VALUE;

	Linebreaks eolTreatment = LINEBREAKS_DEFAULT;
	Multiline multilineTreatment = MULTILINE_DEFAULT;
	
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private String signature;
    private String loadLocation;
    private boolean[] mRequiredColumns = null;
    private boolean mRequiredColumnsInitialized = false;
    
	final Logger logger = Logger.getLogger(getClass().getName());
	@SuppressWarnings("rawtypes")
    protected RecordReader in = null;    

	
	// For replacing LF with CRLF (Unix --> Windows end-of-line convention):
	Pattern loneLFDetectorPattern = Pattern.compile("([^\r])\n", Pattern.DOTALL | Pattern.MULTILINE);
	Matcher loneLFDetector = loneLFDetectorPattern.matcher("");
	
	// For removing CR (Windows --> Unix):
	Pattern CRLFDetectorPattern = Pattern.compile("\r\n", Pattern.DOTALL | Pattern.MULTILINE);
	Matcher CRLFDetector = CRLFDetectorPattern.matcher("");
	

	
	// Pig Storage with COMMA as delimiter:
		TupleFactory tupleMaker = TupleFactory.getInstance();
	
	/*-----------------------------------------------------
	| Constructors 
	------------------------*/
		
		
    public CSVExcelStorage() {
    	super(new String(new byte[] {FIELD_DEL}));
    }
    
    /**
     * Constructs a CSVExcel load/store that uses specified string as a field delimiter.
     * 
     * @param delimiter
     *            the single byte character that is used to separate fields.
     *            ("," is the default.)
     */
    public CSVExcelStorage(String delimiter) {
    	super(delimiter);
        initializeInstance(delimiter, MULTILINE_DEFAULT_STR, LINEBREAKS_DEFAULT_STR);
    }
		
    /**
     * Constructs a CSVExcel load/store that uses specified string 
     * as a field delimiter, and allows specification whether to handle
     * line breaks within fields. For NO_MULTILINE, every line break 
     * will be considered an end-of-record. for YES_MULTILINE, fields
     * may include newlines, as long as the fields are enclosed in 
     * double quotes.  
     * 
     * @param delimiter
     *            the single byte character that is used to separate fields.
     *            ("," is the default.)
     * @param multilineStr
     * 			  "YES_MULTILINE" or "NO_MULTILINE"
     *            ("NO_MULTILINE is the default.)
     */
    public CSVExcelStorage(String delimiter, String multilineStr) {
    	super(delimiter);
        initializeInstance(delimiter, multilineStr, LINEBREAKS_DEFAULT_STR);
    }

    /**
     * Constructs a CSVExcel load/store that uses specified string 
     * as a field delimiter, provides choice to manage multi-line 
     * fields or not, and specifies chars used for end of line.
     * 
     * The eofTreatment parameter is only relevant for STORE():
     *       For "UNIX", newlines will be stored as LF chars
     *       For "WINDOWS", newlines will be stored as CRLF
     * 
     * @param delimiter
     *            the single byte character that is used to separate fields.
     *            ("," is the default.)
     * @param String 
     * 			  "YES_MULTILINE" or "NO_MULTILINE"
     *            ("NO_MULTILINE is the default.)
     * @param eofTreatment
     * 			  "UNIX", "WINDOWS", or "NOCHANGE" 
     *            ("NOCHANGE" is the default.)
     */
    public CSVExcelStorage(String delimiter, String multilineStr,  String theEofTreatment) {
    	super(delimiter);
    	initializeInstance(delimiter, multilineStr, theEofTreatment);
    }

    
    private void initializeInstance(String delimiter, String multilineStr, String theEofTreatment) {
        FIELD_DEL = StorageUtil.parseFieldDel(delimiter);
        multilineTreatment = canonicalizeMultilineTreatmentRequest(multilineStr);
        eolTreatment = canonicalizeEOLTreatmentRequest(theEofTreatment);
    }
    
    private Multiline canonicalizeMultilineTreatmentRequest(String theMultilineStr) {
    	if (theMultilineStr.equalsIgnoreCase("YES_MULTILINE")) {
    		return Multiline.YES;
    	}
    	if (theMultilineStr.equalsIgnoreCase("NO_MULTILINE")) {
    		return Multiline.NO;
    	}
    	return MULTILINE_DEFAULT;
    }
    
    private Linebreaks canonicalizeEOLTreatmentRequest (String theEolTreatmentStr) {
    	if (theEolTreatmentStr.equalsIgnoreCase("Unix"))
    		return Linebreaks.UNIX;
    	if (theEolTreatmentStr.equalsIgnoreCase("Windows"))
    		return Linebreaks.WINDOWS;
    	return LINEBREAKS_DEFAULT;
    }
    
    // ---------------------------------------- STORAGE -----------------------------
    
	/*-----------------------------------------------------
	| putNext()
	------------------------*/
				
    /* (non-Javadoc)
     * @see org.apache.pig.builtin.PigStorage#putNext(org.apache.pig.data.Tuple)
     * 
     * Given a tuple that corresponds to one record, write
     * it out as CSV, converting among Unix/Windows line
     * breaks as requested in the instantiation. Also take
     * care of escaping field delimiters, double quotes,
     * and linebreaks embedded within fields,
     * 
     */
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
    		
    		// If any field delimiters are in the field, or if we did replace
    		// any double quotes with a pair of double quotes above,
    		// or if the string includes a newline character (LF:\n:0x0A)
    		// and we are to allow newlines in fields,
    		// then the entire field must be enclosed in double quotes:
    		embeddedNewlineIndex =  fieldStr.indexOf(LINEFEED);
    		
    		if ((fieldStr.indexOf(FIELD_DEL) != -1) || 
    			(fieldStr.indexOf(DOUBLE_QUOTE) != -1) ||
    			(multilineTreatment == Multiline.YES) && (embeddedNewlineIndex != -1))  {
    			fieldStr = "\"" + fieldStr + "\"";
    		}
    		
    		// If requested: replace any Linefeed-only (^J), with LF-Newline (^M^J),
    		// This is needed for Excel to recognize a field-internal 
    		// new line:

    		if ((eolTreatment != Linebreaks.NOCHANGE) && (embeddedNewlineIndex != -1)) {
    			if (eolTreatment == Linebreaks.WINDOWS) {
    				loneLFDetector.reset(fieldStr);
    				loneLFDetector.matches();
    				fieldStr = loneLFDetector.replaceAll("$1\r\n");
    			} else if (eolTreatment == Linebreaks.UNIX) {
    				CRLFDetector.reset(fieldStr);
    				fieldStr = CRLFDetector.replaceAll("\n");
    			}
    		}

    		mProtoTuple.add(fieldStr);    		
    	}
    	// If Windows line breaks are requested, append 
    	// a newline (0x0D a.k.a. ^M) to the last field
    	// so that the row termination will end up being
    	// \r\n, once the superclass' putNext() method
    	// is done below:

    	if ((eolTreatment == Linebreaks.WINDOWS) && (fieldStr != null))
    		mProtoTuple.set(mProtoTuple.size() - 1, fieldStr + "\r"); 

    	Tuple resTuple = tupleMaker.newTuple(mProtoTuple);
    	// Checking first for debug enabled is faster:
    	if (logger.isDebugEnabled())
    		logger.debug("Row: " + resTuple);
    	super.putNext(resTuple);
    }
	
    // ---------------------------------------- LOADING  -----------------------------	
	
    @Override
    public Tuple getNext() throws IOException {
        mProtoTuple = new ArrayList<Object>();

        boolean inField = false;
        boolean inQuotedField = false;
        boolean evenQuotesSeen = true;
        boolean sawEmbeddedRecordDelimiter = false;
        byte[] buf = null;
        
        if (!mRequiredColumnsInitialized) {
            if (signature != null) {
                Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
                mRequiredColumns = (boolean[])ObjectSerializer.deserialize(p.getProperty(signature));
            }
            mRequiredColumnsInitialized = true;
        }
        // Note: we cannot factor out the check for nextKeyValue() being null,
        // because that call overwrites buf with the new line, which is
        // bad if we have a field with a newline.

        try {
        	int recordLen = 0;
        	int fieldID = 0;
        	
        	while (sawEmbeddedRecordDelimiter || fieldID == 0) {
        		Text value = null;
        		if (sawEmbeddedRecordDelimiter) {
        			
        			// Save the length of the record so far, plus one byte for the 
        			// record delimiter (usually newline) that's embedded in field 
        			// we were working on before falling into this branch:
        			int prevLineLen = recordLen + 1;
        			
        			// Save previous line (the one with the field that has the newline) in a new array.
        			// The last byte will be random; we'll fill in the embedded
        			// record delimiter (usually newline) below:
        			byte[] prevLineSaved = Arrays.copyOf(buf, prevLineLen);
        			prevLineSaved[prevLineLen - 1] = RECORD_DEL;
        			
        			// Read the continuation of the record, unless EOF:
        			if (!in.nextKeyValue()) {
        				return null;
        			}                                                                                           
        			value = (Text) in.getCurrentValue();
        			recordLen = value.getLength();
        			// Grab the continuation's bytes:
        			buf = value.getBytes();
        			
        			// Combine the previous line and the continuation into a new array.
        			// The following copyOf() does half the job: it allocates all the
        			// space, and also copies the previous line into that space:
        			byte[] prevLineAndContinuation = Arrays.copyOf(prevLineSaved, prevLineLen + recordLen);
        			
        			
        			// Now append the continuation. Parms: fromBuf, fromStartPos, toBuf, toStartPos, lengthToCopy:
        			System.arraycopy(buf, 0, prevLineAndContinuation, prevLineLen, recordLen);
        			
        			// We'll work with the combination now:
        			buf = prevLineAndContinuation;
        			
        			// Do the whole record over from the start:
        			mProtoTuple.clear();
        			inField = false;
        			inQuotedField = false;
        			evenQuotesSeen = true;
        			sawEmbeddedRecordDelimiter = false;
        			fieldID = 0;
        			recordLen = prevLineAndContinuation.length;
        			
        		} else {
        			// Previous record finished cleanly: start with the next record,
        			// unless EOF:
        			if (!in.nextKeyValue()) {
        				return null;
        			}                                                                                           
        			value = (Text) in.getCurrentValue();
        			buf = value.getBytes();
        			fieldID = 0;
        			recordLen = value.getLength();
        		}
        		
        		sawEmbeddedRecordDelimiter = false;

        		// Skipping over a double double-quote?
        		boolean skipChar = false;

        		ByteBuffer fieldBuffer = ByteBuffer.allocate(recordLen);

        		for (int i = 0; i < recordLen; i++) {
        			if (skipChar) {
        				skipChar = false;
        				continue;
        			}
        			byte b = buf[i];
        			inField = true;
        			if (inQuotedField) {
        				if (b == DOUBLE_QUOTE) {
        					// Does a double quote immediately follow?
        					if ((i < recordLen-1) && (buf[i+1] == DOUBLE_QUOTE)) {
        						fieldBuffer.put(b);
        						skipChar = true;
        						continue;
        					}
        					evenQuotesSeen = !evenQuotesSeen;
        					if (evenQuotesSeen) {
        						fieldBuffer.put(DOUBLE_QUOTE);
        					}
        				} else if (i == recordLen - 1) {
        					// This is the last char we read from the input stream,
        					// but we have an open double quote.
        					// We either have a run-away quoted field (i.e. a missing
        					// closing field in the record), or we have a field with 
        					// a record delimiter in it. We assume the latter,
        					// and cause the outer while loop to run again, reading
        					// more from the stream. Write out the delimiter:
        					fieldBuffer.put(b);
        					sawEmbeddedRecordDelimiter = true;
        					continue;
        				} else
        					if (!evenQuotesSeen &&
        							(b == FIELD_DEL || b == RECORD_DEL)) {
        						inQuotedField = false;
        						inField = false;
        						readField(fieldBuffer, fieldID++);
        					} else {
        						fieldBuffer.put(b);
        					}
        			} else if (b == DOUBLE_QUOTE) {
        				// Does a double quote immediately follow?                	
        				if ((i < recordLen-1) && (buf[i+1] == DOUBLE_QUOTE)) {
        					fieldBuffer.put(b);
        					skipChar = true;
        					continue;
        				}
        				inQuotedField = true;
        				evenQuotesSeen = true;
        			} else if (b == FIELD_DEL) {
        				inField = false;
        				readField(fieldBuffer, fieldID++); // end of the field
        			} else {
        				evenQuotesSeen = true;
        				fieldBuffer.put(b);
        			}
        		}
        		if (inField && !sawEmbeddedRecordDelimiter) readField(fieldBuffer, fieldID++);
        	} // end while

        } catch (InterruptedException e) {
        	int errCode = 6018;
        	String errMsg = "Error while reading input";
        	throw new ExecException(errMsg, errCode, 
        			PigException.REMOTE_ENVIRONMENT, e);
        }

        Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
        return t;
    }

    private void readField(ByteBuffer buf, int fieldID) {
        if (mRequiredColumns==null || (mRequiredColumns.length>fieldID && mRequiredColumns[fieldID])) {
            byte[] bytes = new byte[buf.position()];
            buf.rewind();
            buf.get(bytes, 0, bytes.length); 
            mProtoTuple.add(new DataByteArray(bytes));
        }
        buf.clear();
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        loadLocation = location;
        FileInputFormat.setInputPaths(job, location);        
    }

    @SuppressWarnings("rawtypes")
    @Override
    public InputFormat getInputFormat() {
        if(loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
            return new Bzip2TextInputFormat();
        } else {
            return new PigTextInputFormat();
        }
    }

    @Override
    public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) {
        in = reader;
    }

    @Override
    public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
        if (requiredFieldList == null)
            return null;
        if (requiredFieldList.getFields() != null)
        {
            int lastColumn = -1;
            for (RequiredField rf: requiredFieldList.getFields())
            {
                if (rf.getIndex()>lastColumn)
                {
                    lastColumn = rf.getIndex();
                }
            }
            mRequiredColumns = new boolean[lastColumn+1];
            for (RequiredField rf: requiredFieldList.getFields())
            {
                if (rf.getIndex()!=-1)
                    mRequiredColumns[rf.getIndex()] = true;
            }
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            try {
                p.setProperty(signature, ObjectSerializer.serialize(mRequiredColumns));
            } catch (Exception e) {
                throw new RuntimeException("Cannot serialize mRequiredColumns");
            }
        }
        return new RequiredFieldResponse(true);
    }

    @Override
    public void setUDFContextSignature(String signature) {
        this.signature = signature; 
    }

    @Override
    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }

	
/*	
	// ------------------------------------------ Testing ---------------------
    
    static ArrayList<String> doTests() {

    	PigServer pserver = null;
    	Properties props = new Properties();
    	ArrayList<String> testMsgs = new ArrayList<String>();
    	
    	String testStrComma = "John,Doe,10\n" +
    					      "Jane, \"nee, Smith\",20\n" +
    					      "\"Mac \"\"the knife\"\"\",Cohen,30\n" +
    					      "\"Conrad\n" +
    					      "Emil\",Dinger,40\n";
    	
    	String testStrTab   = "John\tDoe\t50\n" +
    						  "\"Foo and CR last\n" +
    						  "bar.\"\t\t\n" +
    						  "Frank\tClean\t70";
    	
		try {
			props.setProperty("pig.usenewlogicalplan", "false");
			pserver = new PigServer(ExecType.MAPREDUCE, props);
		} catch (ExecException e) {
			e.printStackTrace();
		}
		
    	// ***************************  SETUP ********************
		
		
		File testCommaCSVFileIn = null;
		File testTabCSVFileIn = null;
		File testCSVFileOut = null;
		try {
			testCommaCSVFileIn = File.createTempFile("csvExcelStorageTestIn",".csv");
			//testCommaCSVFileIn.deleteOnExit();
			BufferedWriter out = new BufferedWriter(new FileWriter(testCommaCSVFileIn.getAbsolutePath()));
			System.out.println("Writing comma-separated test file to " + testCommaCSVFileIn.getAbsolutePath());
			out.write(testStrComma);
			out.close();

			testTabCSVFileIn = File.createTempFile("csvExcelStorageTestIn",".csv");
			//testTabCSVFileIn.deleteOnExit();
			out = new BufferedWriter(new FileWriter(testTabCSVFileIn.getAbsolutePath()));
			System.out.println("Writing tab-separated test file to " + testTabCSVFileIn.getAbsolutePath());
			out.write(testStrTab);
			out.close();
			
			// Get a tmp file name for the output, but ensure that this
			// file doesn't exist by the time Pig wants to store to it.
			testCSVFileOut = File.createTempFile("csvExcelStorageTestOut",".csv");
			testCSVFileOut.delete();
			
		} catch (IOException e1) {
			System.out.println("Cannot write test file.");
			System.exit(-1);
		}
		
		// Replace Windows backslash with forward slashes in these file paths:
		String testCommaInFileName = testCommaCSVFileIn.getAbsolutePath().replaceAll("\\\\", "/");
		String testTabInFileName = testTabCSVFileIn.getAbsolutePath().replaceAll("\\\\", "/");
		String testOutFileName = testCSVFileOut.getAbsolutePath().replaceAll("\\\\", "/");
		
		testMsgs.add("The comma-separated test file is at: " + testCommaInFileName);
		testMsgs.add("The tab-separated test file is at: " + testTabInFileName);
		
		try {

			// ***************************  Load test files in once ********************
			
			pserver.registerJar("contrib/PigIR.jar");
			
			System.out.println("Loading comma test file from " + testCommaInFileName);
			pserver.registerQuery("B = LOAD '" + testCommaInFileName + "' " +
			                      "USING pigir.pigudf.CSVExcelStorage(',');");
						
			System.out.println("Loading tab test file from " + testTabInFileName);
			pserver.registerQuery("C = LOAD '" + testTabInFileName + "' " +
			                      "USING pigir.pigudf.CSVExcelStorage();");
			
			
			
			// ***************************  Comma, YES_MULTILINE, NOCHANGE Linebreaks ********************
			
			
			System.out.println("Storing result for comma/YES_MULTILINE to: " + testOutFileName);
			pserver.registerQuery("STORE B INTO '" + testOutFileName + "' USING " +
					"pigir.pigudf.CSVExcelStorage(',', 'YES_MULTILINE');");
		
			
			testMsgs.add("Comma, YES_MULTILINE, " + LINEBREAKS_DEFAULT_STR + " test result in: " + testOutFileName);
			
			
			// Try to list the result file:
			BufferedReader result = null;
			try {
				result = new BufferedReader(new FileReader(testCSVFileOut.getAbsoluteFile()));
				String line;
				while ((line = result.readLine()) != null) {
					System.out.println(line);
				}
			} catch (Exception e) {
				System.out.println("Could not print result file: " + e.getMessage() + ". Grab it manually via file names listed at end.");
			}
			
			// ***************************  TAB, YES_MULTILINE, NOCHANGE Linebreaks ********************

			try {
				// Get a new tmp file name, but ensure that this
				// file doesn't exist by the time Pig wants to store to it.
				testCSVFileOut = File.createTempFile("csvExcelStorageTestOut",".csv");
				testCSVFileOut.delete();
				testOutFileName = testCSVFileOut.getAbsolutePath().replaceAll("\\\\", "/");
			} catch (IOException e1) {
				System.out.println("Cannot create output test file.");
				System.exit(-1);
			}
			
			System.out.println("Storing result for tab/YES_MULTILINE/Default EOL treatment to: " + testOutFileName);
			pserver.registerQuery("STORE C INTO '" + testOutFileName + "' USING " +
					"pigir.pigudf.CSVExcelStorage('\t', 'YES_MULTILINE');");
			
			testMsgs.add("TAB, YES_MULTILINE, " + LINEBREAKS_DEFAULT_STR + " test result in: " + testOutFileName);
			
			try {
				result = new BufferedReader(new FileReader(testCSVFileOut.getAbsoluteFile()));
				String line;
				while ((line = result.readLine()) != null) {
					System.out.println(line);
				}
			} catch (Exception e) {
				System.out.println("Could not print result file: " + e.getMessage() + ". Grab it manually via file names listed at end.");
			}
			
			// ***************************  Comma, YES_MULTILINE, WINDOWS Linebreaks ********************
			
			
			try {
				// Get a new tmp file name, but ensure that this
				// file doesn't exist by the time Pig wants to store to it.
				testCSVFileOut = File.createTempFile("csvExcelStorageTestOut",".csv");
				testCSVFileOut.delete();
				testOutFileName = testCSVFileOut.getAbsolutePath().replaceAll("\\\\", "/");
			} catch (IOException e1) {
				System.out.println("Cannot create output test file.");
				System.exit(-1);
			}
			
			System.out.println("Storing result for comma/YES_MULTILINE/WINDOWS to: " + testOutFileName);
			pserver.registerQuery("STORE B INTO '" + testOutFileName + "' USING " +
					"pigir.pigudf.CSVExcelStorage(',', 'YES_MULTILINE', 'WINDOWS');");
			
			testMsgs.add("Comma, YES_MULTILINE, WINDOWS test result in: " + testOutFileName);
			
			try {
				result = new BufferedReader(new FileReader(testCSVFileOut.getAbsoluteFile()));
				String line;
				while ((line = result.readLine()) != null) {
					System.out.println(line);
				}
			} catch (Exception e) {
				System.out.println("Could not print result file: " + e.getMessage() + ". Grab it manually via file names listed at end.");
			}
			
			
			// ***************************  Comma, NO_MULTILINE, WINDOWS Linebreaks ********************
			
			try {
				// Get a new tmp file name, but ensure that this
				// file doesn't exist by the time Pig wants to store to it.
				testCSVFileOut = File.createTempFile("csvExcelStorageTestOut",".csv");
				testCSVFileOut.delete();
				testOutFileName = testCSVFileOut.getAbsolutePath().replaceAll("\\\\", "/");
			} catch (IOException e1) {
				System.out.println("Cannot create output test file.");
				System.exit(-1);
			}
			
			System.out.println("Storing result for comma/NO_MULTILINE/WINDOWS to: " + testOutFileName);
			pserver.registerQuery("STORE B INTO '" + testOutFileName + "' USING " +
					"pigir.pigudf.CSVExcelStorage(',', 'NO_MULTILINE', 'WINDOWS');");
			
			testMsgs.add("Comma, NO_MULTILINE, WINDOWS line break test result in: " + testOutFileName);
			
			try {
				result = new BufferedReader(new FileReader(testCSVFileOut.getAbsoluteFile()));
				String line;
				while ((line = result.readLine()) != null) {
					System.out.println(line);
				}
			} catch (Exception e) {
				System.out.println("Could not print result file: " + e.getMessage() + ". Grab it manually via file names listed at end.");
			}
			
		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage());
		} 
		return testMsgs;
	}
    
    
	public static void main(String[] args) {
		ArrayList<String> resMsgs = CSVExcelStorage.doTests();
		System.out.println("\n");
		for (String msg : resMsgs)
			System.out.println(msg);
	}
*/
}
