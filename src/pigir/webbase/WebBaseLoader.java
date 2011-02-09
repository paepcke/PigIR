package pigir.webbase;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
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

import pigir.MultiTypeProperties;
import pigir.warc.WarcRecord;

public class WebBaseLoader extends LoadFunc implements LoadPushDown {

	public final Logger logger = Logger.getLogger(getClass().getName());
	
	private MultiTypeProperties wbJobProperties = 
		new MultiTypeProperties(UDFContext.getUDFContext().getUDFProperties(getClass()));
	private final int NUM_OUTPUT_COLUMNS = 5;
	private final int CONTENT_COL_INDEX = 5;
	
    // Vector with true wbRecordReader each position that corresponds to a
    // field that this loader is to include wbRecordReader its return tuples: 
    private boolean[] mRequiredColumns = null;
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private int numColsToReturn = 5; // URL, Date, Position wbRecordReader site, docID within the delivery. 
    private boolean mRequiredColumnsInitialized = false;
    private WbRecord wbRec = null;

    private String signature;
    
    @SuppressWarnings({ "serial", "unused" })
	private HashSet<String> wbMimeTypes = new HashSet<String>() {
    	{
    		add("audio");
    		add("image");
    		add("text");
    		add("unknown");
    	}
    };

    private String crawlName = null;
    private String crawlType = null;
    private DistributorContact distributorContact = null;
    
    protected WbRecordReader wbRecordReader = null;
    
	/*-----------------------------------------------------
	| Constructors 
	------------------------*/
    
    public WebBaseLoader() {
    }
    
	/*-----------------------------------------------------
	| relativeToAbsolutePath()
	------------------------*/
    /**
     * This method is called by Pig early on in the load process. The passed-in
     * location parameter is the quoted (first) part of the Pig load command:
     *    LOAD 'foo' USING...
     * We expect location to be a colon-separated pair <crawlName>:<crawlType>.
     * 
     * Pig insists that what is returned looks like an absolute path, even
     * though we are not dealing with files in this loader. Whatever we return
     * will later be passed to setLocation(); Here we just split the crawl name
     * from the crawl type and remember those values.
     *  
     * @param crawlNameType Example: LOAD 'crawled_hosts.gov-12-2009.tx:text' ...;
     */
    
    @Override
    public String relativeToAbsolutePath(String location, Path curDir) 
    throws IOException {      

    	return "/" + location;
    }   
 
	/*-----------------------------------------------------
	| getNext() 
	------------------------*/
    
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
            		String headerVal = wbRec.get(headerKey);
            		mProtoTuple.add(fldConstructor.newInstance(headerVal));
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
    
	/*-----------------------------------------------------
	| getFeatures()
	------------------------*/
	
	@Override
	public List<OperatorSet> getFeatures() {
		return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
	}

	/*-----------------------------------------------------
	| pushProjection()
	------------------------*/

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
    
	/*-----------------------------------------------------
	| setLocation() 
	------------------------*/
	
	/* (non-Javadoc)
	 * @see org.apache.pig.LoadFunc#setLocation(java.lang.String, org.apache.hadoop.mapreduce.Job)
	 * Location is expected to be a string /<crawlName>[:<numPages>], which is 
	 * returned from our relativeToAbsolutePath() method (which is called earlier). If 
	 * numPages is omitted, all pages is assumed. 
	 * 
	 * The leading'/' is only to fool the LoadFunc superclass' call to getAbsolutePath(). That class
	 * has a file mindset and checks whether the 'location' has a leading '/'.
	 * 
	 * 
	 */
	
	@Override
	public void setLocation(String location, Job theJob) throws IOException {
		
		// Get rid of the '/'
		location = location.substring(1);
    	String[] crawlAndType = location.split(":");
    	if (crawlAndType.length > 2) {
    		String errMsg = "Badly formatted crawl name/numPages string. Should be <name>:<numPages>. Was " + location; 
    		logger.error(errMsg);
    		throw new IOException(errMsg);
    	}
    	
    	crawlName = crawlAndType[0].trim();
    	
    	if (crawlAndType.length > 1) {
    		
    	}
    	
    	crawlType = crawlAndType[1].trim();
    	distributorContact = DistributorContact.getCrawlDistributorAddr(crawlName, crawlType);
				
		int numReducers = theJob.getNumReduceTasks();
		if (numReducers <= 0)
			wbJobProperties.setInt(Constants.NUM_OF_CPUS_PROP_NAME, Constants.DEFAULT_NUM_OF_SPLITS);
		else
			wbJobProperties.setInt(Constants.NUM_OF_CPUS_PROP_NAME, theJob.getNumReduceTasks());

		// Make the distributor demon contact available 
		// to all the splits via the job property list.
		wbJobProperties.setProperty(Constants.WB_DISTRIBUTOR_DEMON_KEY, distributorContact.toConfigStr());
	}
	
	/*-----------------------------------------------------
	| setUDFContextSignature() 
	------------------------*/
	
    @Override
    public void setUDFContextSignature(String signature) {
        this.signature = signature; 
    }

	/*-----------------------------------------------------
	| getInputFormat() 
	------------------------*/
	
	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() throws IOException {
		return new WbInputFormat(wbJobProperties);
	}

	
	/*-----------------------------------------------------
	| prepareToRead() 
	------------------------*/
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		wbRecordReader = (WbRecordReader) reader;
	}
	
	/*-----------------------------------------------------
	| getDistributorContact() 
	------------------------*/
	
	public DistributorContact getDistributorContact() {
		return distributorContact;
	}
}
