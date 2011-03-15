package pigir.pigudf;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.ExecType;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import pigir.Common;

/**
 * A load function based on PigStorage that implements part of the CSV "standard"
 * This loader properly supports double-quoted fields that contain commas and other
 * double-quotes escaped with backslashes.
 *
 * The following fields are all parsed as one tuple, per each line
 * "the man, he said ""hello"""
 * "one,two,three"
 *
 * This version supports pig 0.7+
 *
 */

public class CSVExcelLoader extends FileInputLoadFunc implements LoadPushDown {

    @SuppressWarnings("rawtypes")
    protected RecordReader in = null;    

    protected static final Log LOG = LogFactory.getLog(CSVExcelLoader.class);
    private static final byte DOUBLE_QUOTE = '"';
    private static final byte FIELD_DEL = ',';
    private static final byte RECORD_DEL = '\n';

    long end = Long.MAX_VALUE;

    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();

    private String signature;
    private String loadLocation;

    private boolean[] mRequiredColumns = null;

    private boolean mRequiredColumnsInitialized = false;

    public CSVExcelLoader() {
    }

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
        		// ****
        		// Problems: Need to sneak the CR in between the prev line and the new line.
        		//           And: why does it go around more than once?
        		//                why does it lose the last line of the string.csv?
        		//****
        		Text value = null;
        		if (sawEmbeddedRecordDelimiter) {
        			
        			int prevLineLen = recordLen;
        			
        			// Save previous line (the one with the field that has the newline) in a new array:
        			byte[] prevLineSaved = Arrays.copyOf(buf, prevLineLen);
        			
        			//*******************
        			System.out.println("P1: buf.length:" + buf.length);
        			System.out.println("P1: prevLineSave.length:" + prevLineSaved.length);
        			//*******************
        			
        			// Read the continuation of the record, unless EOF:
        			if (!in.nextKeyValue()) {
        				return null;
        			}                                                                                           
        			value = (Text) in.getCurrentValue();
        			recordLen = value.getLength();
        			// Grab the continuation's bytes:
        			buf = value.getBytes();
        			
        			//*******************
        			System.out.println("P2: buf.length:" + buf.length);
        			System.out.println("P2: prevLineSave.length:" + prevLineSaved.length);
        			//*******************
        			
        			// Combine the previous line and the continuation into a new array.
        			// The following copyOf() does half the job: it allocates all the
        			// space, and also copies the previous line into that space:
        			byte[] prevLineAndContinuation = Arrays.copyOf(prevLineSaved, prevLineLen + recordLen);
        			
        			//*******************
        			System.out.println("P3: buf.length:" + buf.length);
        			System.out.println("P3: prevLineSave.length:" + prevLineSaved.length);
        			System.out.println("P3: prevLineAndContinuation.length:" + prevLineAndContinuation.length);
        			//*******************

        			
        			// Now append the continuation. Parms: fromBuf, fromStartPos, toBuf, toStartPos, lengthToCopy:
        			System.arraycopy(buf, 0, prevLineAndContinuation, prevLineLen, recordLen);
        			
        			//*******************
        			System.out.println("P4: buf.length:" + buf.length);
        			System.out.println("P4: prevLineSave.length:" + prevLineSaved.length);
        			System.out.println("P4: prevLineAndContinuation.length:" + prevLineAndContinuation.length);
        			//*******************
        			
        			// We'll work with the combination now:
        			buf = prevLineAndContinuation;
        			
        			//*******************
        			System.out.println("P4: buf.length:" + buf.length);
        			//*******************
        			
        			// Do the whole record over from the start:
        			mProtoTuple.clear();
        			inField = false;
        			inQuotedField = false;
        			evenQuotesSeen = true;
        			sawEmbeddedRecordDelimiter = false;
        			fieldID = 0;
        			
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
        					// a record delimiter in it (\n). We assume the latter,
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
    public InputFormat getInputFormat() throws IOException {
        if(loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
            return new Bzip2TextInputFormat();
        } else {
            return new PigTextInputFormat();
        }
    }

    @Override
    public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split)
    throws IOException {
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

    //--------------------  Testing -----------------------------------
    
    void doTests() {

    	PigServer pserver = null;
    	Properties props = new Properties();
    	
		try {
			props.setProperty("pig.usenewlogicalplan", "false");
			pserver = new PigServer(ExecType.MAPREDUCE, props);
		} catch (ExecException e) {
			e.printStackTrace();
		}
		try {
			
			//Map<String, String> env = System.getenv();
			// URI piggybankPath = new File(env.get("PIG_HOME"),
			URI piggybankPath = new File("E:/Users/Paepcke/Software/Hadoop/pig-0.8.0/contrib/piggybank/java/piggybank.jar").toURI();
			pserver.registerJar(piggybankPath.toString());
			pserver.registerJar("contrib/PigIR.jar");
			
			// Matrix of integers: 1,2,3
			//                     4,5,6
			pserver.registerQuery("A = LOAD 'E:/Users/Paepcke/dldev/TestData/integers.csv' " +
								   "USING org.apache.pig.piggybank.storage.CSVLoader;");
			Common.print(pserver, "A");
			pserver.registerQuery("STORE A INTO 'E:/Users/Paepcke/dldev/TestData/integersBackOut.csv' USING pigir.pigudf.CSVExcelWriter();");
			
			// Matrix of hairy stuff:
  			//        Andreas, Paepcke, 10
			//		  Inez,Griesbaum,20
			//		  Johnny "the knive", Conley, 40
			//		  Conrad, "blue, gray man", 50
			
			pserver.registerQuery("B = LOAD 'E:/Users/Paepcke/dldev/TestData/strings.csv' " +
								   "USING pigir.pigudf.CSVExcelLoader;");
			Common.print(pserver, "B");
			pserver.registerQuery("STORE B INTO 'E:/Users/Paepcke/dldev/TestData/stringsBackOut.csv' USING pigir.pigudf.CSVExcelWriter();");
			
		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage());
		}
	}
    
    
	public static void main(String[] args) {
		new CSVExcelLoader().doTests();
	}

}
