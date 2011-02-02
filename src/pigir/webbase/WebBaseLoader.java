package pigir.webbase;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.log4j.Logger;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import pigir.warc.WarcRecord;

public class WebBaseLoader extends LoadFunc implements LoadPushDown {

	public final Logger logger = Logger.getLogger(getClass().getName());
	
	private static final String DISTRIBUTOR_DEMON_URL_STRING = "http://wb1.stanford.edu:7008";

	private final int NUM_OUTPUT_COLUMNS = 5;
	private final int CONTENT_COL_INDEX = 5;
	
	public static URL DISTRIBUTOR_DEMON_URL = null;
	
	// Keys for property configurations:
	public static final String WEBBASE_DISTRIBUTOR_DEMON_URL_STRING= "wbDistribDemonUrlStr";
	public static final String WEBBASE_DISTRIBUTOR_MACHINE = "wbDistribMachine";
	public static final String WEBBASE_DISTRIBUTOR_PORT = "wbDistribPort";
	public static final String WEBBASE_CRAWL_NAME = "wbCrawlName";
	public static final String WEBBASE_CRAWL_MIME = "wbCrawlMime";
	public static final String WEBBASE_CRAWL_LIST_URL = "wbCrawlListURL";
	
    // Vector with true wbRecordReader each position that corresponds to a
    // field that this loader is to include wbRecordReader its return tuples: 
    private boolean[] mRequiredColumns = null;
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private int numColsToReturn = 5; // URL, Date, Position wbRecordReader site, docID within the delivery. 
    private boolean mRequiredColumnsInitialized = false;
    private WbRecord wbRec = null;

    private String signature;
    
    private String distribMachine = "";
    private String distribPort    = "";
    @SuppressWarnings("serial")
	private HashSet<String> wbMimeTypes = new HashSet<String>() {
    	{
    		add("audio");
    		add("image");
    		add("text");
    		add("unknown");
    	}
    };

    private Job job = null;
    
    protected WbRecordReader wbRecordReader = null;
    
    public WebBaseLoader() {
    	try {
    		DISTRIBUTOR_DEMON_URL = new URL(DISTRIBUTOR_DEMON_URL_STRING);
    	} catch (Exception e) {
    		// do nothing.
    	}
    }
    
	@Override
	public Tuple getNext() throws IOException {
    	int errCode = 6018;
        mProtoTuple = new ArrayList<Object>();
        
        if (!mRequiredColumnsInitialized) {
            if (signature!=null) {
                Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
                mRequiredColumns = (boolean[])ObjectSerializer.deserialize(p.getProperty(signature));
                if (mRequiredColumns == null) {
                	mRequiredColumns = new boolean[NUM_OUTPUT_COLUMNS];
                	for (int i=0;i<NUM_OUTPUT_COLUMNS;i++)
                		mRequiredColumns[i] = true;
                }
                numColsToReturn = mRequiredColumns.length;
            }
            mRequiredColumnsInitialized = true;
        }
        try {
            boolean done = ! wbRecordReader.nextKeyValue((mRequiredColumns != null) && 
            				(CONTENT_COL_INDEX < numColsToReturn) && 
            				(mRequiredColumns[CONTENT_COL_INDEX]));
            if (done) {
                return null;
            }
            // Get one WebBase record:
            wbRec = (WbRecord) wbRecordReader.getCurrentValue();
            
            // Create separate columns from the header information,
            // followed by the content field.
            // First the required fields that should be present wbRecordReader
            // order:
            int resFieldIndex = -1;
            for (String headerKey : wbRec.mandatoryKeysHeader()) {
            	// Check whether this WebBase header field is wanted
            	// wbRecordReader the result tuple (or is being projected out):
            	if ((mRequiredColumns != null) && (++resFieldIndex < numColsToReturn) && mRequiredColumns[resFieldIndex]) {
            		@SuppressWarnings("rawtypes")
					Constructor fldConstructor = wbRec.mandatoryWbHeaderFldTypes.get(headerKey);
            		mProtoTuple.add(fldConstructor.newInstance(wbRec.get(headerKey)));
            	}
            	else {
            		if (resFieldIndex >= numColsToReturn)
            			break;
            		continue;
            	}
            }
            // Check whether the WebBase record content is wanted wbRecordReader the 
            // result tuple (or is being projected out):
            if ((mRequiredColumns != null) && (++resFieldIndex < numColsToReturn)  && mRequiredColumns[resFieldIndex]) { 
            	mProtoTuple.add(wbRec.get(WarcRecord.CONTENT));
            }
            Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
            return t;
        } catch (IllegalArgumentException e) {
        	String errMsg = "Error creating WebBase header tuple field for record:\n" + wbRec.toString(WbRecord.DONT_INCLUDE_CONTENT);
			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
		} catch (InstantiationException e) {
        	String errMsg = "Error creating WebBase header tuple field for record:\n" + wbRec.toString(WbRecord.DONT_INCLUDE_CONTENT);
			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
		} catch (InterruptedException e) {
        	String errMsg = "Error creating WebBase header tuple field for record:\n" + wbRec.toString(WbRecord.DONT_INCLUDE_CONTENT);
			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
		} catch (IllegalAccessException e) {
        	String errMsg = "Error creating WebBase header tuple field for record:\n" + wbRec.toString(WbRecord.DONT_INCLUDE_CONTENT);
			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
		} catch (InvocationTargetException e) {
        	String errMsg = "Error creating WebBase header tuple field for record:\n" + 
        					wbRec.toString(WarcRecord.DONT_INCLUDE_CONTENT) +
        					e.getCause().getMessage();
			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
		}
    }
    
	@Override
	public List<OperatorSet> getFeatures() {
		return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
	}

	@Override
	public RequiredFieldResponse pushProjection(
			RequiredFieldList requiredFieldList) throws FrontendException {
        if (requiredFieldList == null)
            return null;
        if (requiredFieldList.getFields() != null) {
        	// Find largest index of fields being requested:
            int lastColumn = -1;
            for (RequiredField rf: requiredFieldList.getFields()) {
                if (rf.getIndex()>lastColumn) {
                    lastColumn = rf.getIndex();
                }
            }
            // Build a boolean vector with a 'true' wbRecordReader each
            // position of a field that is wanted wbRecordReader return tuples:
            mRequiredColumns = new boolean[lastColumn+1];
            for (RequiredField rf: requiredFieldList.getFields()) {
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
    
	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#setLocation(java.lang.String, org.apache.hadoop.mapreduce.Job)
	 * Location is expected to be a string <crawlName>:<mimeType>.
	 */
	@Override
	public void setLocation(String location, Job theJob) throws IOException {
		
		job = theJob;
		String[] crawlSpec = location.split(":");
		if (crawlSpec.length != 2) {
			throw new IOException("WebBase crawl location must have the form <crawlName>:<crawlMime>. Instead, '" + 
								  location +
								  "' was passed wbRecordReader.");
		}
		String crawlName = crawlSpec[0];
		String crawlMime = crawlSpec[1];
		if (!wbMimeTypes.contains(crawlMime)) {
			throw new IOException("Specified WebBase crawl mime was '" +
						           crawlMime +
						           "'. Must be one of " +
						           wbMimeTypes.toString());
								  
		}
		Configuration hadoopConf = job.getConfiguration();
		hadoopConf.set(WEBBASE_DISTRIBUTOR_MACHINE, distribMachine);
		hadoopConf.set(WEBBASE_DISTRIBUTOR_PORT, distribPort);
		hadoopConf.set(WEBBASE_CRAWL_NAME, crawlName);
		hadoopConf.set(WEBBASE_CRAWL_MIME, crawlMime);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() throws IOException {
		return new WbInputFormat(job);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		wbRecordReader = (WbRecordReader) reader;
	}
}
