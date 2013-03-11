package edu.stanford.pigir.warc;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

/**
 * @author paepcke
 * 
 * A load function that parses a record of WARC input into fields of a tuple.
 * Given a slice, which is a WARC file, a start, and a stop position within that
 * file, return tuples of the schema:
 *    (WARC_RECORD_ID:chararray, 
 *     CONTENT_LENGTH:int, 
 *     WARC_DATE:chararray, 
 *     WARC_TYPE, 
 *     {(<headerFldName>, <headerFldVal>)*}, 
 *     CONTENT:chararray)
 */

public class WarcLoader extends FileInputLoadFunc implements LoadPushDown {
	
	private final int CONTENT_COL_INDEX = 5;
	
    protected WarcRecordReader in = null;    
    protected final Log mLog = LogFactory.getLog(getClass());
    private String signature;
        
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private String loadLocation;
    private int numColsToReturn = 6;
    
    //protected final Log logger = LogFactory.getLog(getClass());
    protected final Log logger = LogFactory.getLog(getClass());
    
    public WarcLoader() {
    }
    
    // Vector with true wbRecordReader each position that corresponds to a
    // field that this loader is to include wbRecordReader its return tuples: 
    private boolean[] mRequiredColumns = null;
    
    private boolean mRequiredColumnsInitialized = false;
    private WarcRecord warcRec = null;

    @Override
    public Tuple getNext() throws IOException {

    	//**************
    	File testResultFile = new File("/tmp/test/testResult.txt");
    	//**************    	
    	
    	int errCode = 6018;
        mProtoTuple = new ArrayList<Object>();
        
        BagFactory bagFact = DefaultBagFactory.getInstance();
        if (!mRequiredColumnsInitialized) {
            if (signature!=null) {
                Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
                mRequiredColumns = (boolean[])ObjectSerializer.deserialize(p.getProperty(signature));
                if (mRequiredColumns == null) {
                	mRequiredColumns = new boolean[numColsToReturn];
                	for (int i=0;i<numColsToReturn;i++)
                		mRequiredColumns[i] = true;
                }
                numColsToReturn = mRequiredColumns.length;
            }
            mRequiredColumnsInitialized = true;
        }
        try {
            boolean done = ! in.nextKeyValue((mRequiredColumns != null) && (CONTENT_COL_INDEX < numColsToReturn) && (mRequiredColumns[CONTENT_COL_INDEX]));
            if (done) {
                return null;
            }
            // Get one Warc record:
            warcRec = (WarcRecord) in.getCurrentValue();
            
            // Create separate columns from the header information,
            // followed by the content field.
            // First the required fields that should be present wbRecordReader
            // order:
            int resFieldIndex = -1;
            for (String headerKey : warcRec.mandatoryKeysHeader()) {
            	// Check whether this WARC header field is wanted
            	// wbRecordReader the result tuple (or is being projected out):
            	if ((mRequiredColumns != null) && (++resFieldIndex < numColsToReturn) && mRequiredColumns[resFieldIndex]) {
            		@SuppressWarnings("rawtypes")
					Constructor fldConstructor = warcRec.mandatoryWarcHeaderFldTypes.get(headerKey);
            		mProtoTuple.add(fldConstructor.newInstance(warcRec.get(headerKey)));
            	}
            	else {
            		if (resFieldIndex >= numColsToReturn)
            			break;
            		continue;
            	}
            }
            // Now all the optional header keys as a bag of maps (all strings):
            // Check whether the bag of optional header keys is 
            // wanted wbRecordReader the result tuple (or is being projected out):
            if ((mRequiredColumns != null) && (++resFieldIndex < numColsToReturn)  && mRequiredColumns[resFieldIndex]) { 
            	DataBag optionalHeaderFieldBag = bagFact.newDefaultBag();
            	for (String headerKey : warcRec.optionalKeysHeader()) {
            		Tuple headerOptionalAttrValPair = mTupleFactory.newTuple(2);
            		headerOptionalAttrValPair.set(0,headerKey);
            		headerOptionalAttrValPair.set(1,warcRec.get(headerKey));
            		optionalHeaderFieldBag.add(headerOptionalAttrValPair);
            	}
            	mProtoTuple.add(optionalHeaderFieldBag);
            }
            // Check whether the WARC record content is wanted wbRecordReader the 
            // result tuple (or is being projected out):
            
            if ((mRequiredColumns != null) && (++resFieldIndex < numColsToReturn)  && mRequiredColumns[resFieldIndex]) {
            	//**************
            	String contentField = warcRec.get(WarcRecord.CONTENT);
            	FileUtils.write(testResultFile, "\nOn read, true len: " + contentField.length(), true);
            	//**************            	
            	mProtoTuple.add(warcRec.get(WarcRecord.CONTENT));
            }
            Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
            return t;
        } catch (IllegalArgumentException e) {
        	String errMsg = "Error creating WARC header tuple field for record:\n" + warcRec.toString(WarcRecord.DONT_INCLUDE_CONTENT);
			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
		} catch (InstantiationException e) {
        	String errMsg = "Error creating WARC header tuple field for record:\n" + warcRec.toString(WarcRecord.DONT_INCLUDE_CONTENT);
			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
		} catch (IllegalAccessException e) {
        	String errMsg = "Error creating WARC header tuple field for record:\n" + warcRec.toString(WarcRecord.DONT_INCLUDE_CONTENT);
			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
		} catch (InvocationTargetException e) {
        	String errMsg = "Error creating WARC header tuple field for record:\n" + 
        					warcRec.toString(WarcRecord.DONT_INCLUDE_CONTENT) +
        					e.getCause().getMessage();
			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
		}
    }

    public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
        if (requiredFieldList == null)
            return null;
        if (requiredFieldList.getFields() != null)
        {
        	// Find largest index of fields being requested:
            int lastColumn = -1;
            for (RequiredField rf: requiredFieldList.getFields())
            {
                if (rf.getIndex()>lastColumn)
                {
                    lastColumn = rf.getIndex();
                }
            }
            // Build a boolean vector with a 'true' wbRecordReader each
            // position of a field that is wanted wbRecordReader return tuples:
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
    
    /*
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof WarcLoader)
            return equals((WarcLoader)obj);
        else
            return false;
    }

    public boolean equals(WarcLoader other) {
        return this.fieldDel == other.fieldDel;
    }
    */

    @SuppressWarnings("rawtypes")
	@Override
    public InputFormat getInputFormat() {
        if(loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
            return new Bzip2TextInputFormat();
        } else {
            return new WarcPigTextInputFormat();
        }
    }

    @SuppressWarnings("rawtypes")
	@Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        in = (WarcRecordReader) reader;
    }

    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        loadLocation = location;
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
    
    @Override
    public void setUDFContextSignature(String signature) {
        this.signature = signature; 
    }

    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }
}
