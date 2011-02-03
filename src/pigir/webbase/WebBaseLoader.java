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

import pigir.warc.WarcRecord;

public class WebBaseLoader extends LoadFunc implements LoadPushDown {

	public final Logger logger = Logger.getLogger(getClass().getName());
	
	private static final String DISTRIBUTOR_DEMON_URL_STR = "http://wb1.stanford.edu:7008";
	// TODO: Cannot be hard-wired. Need server to find this URL.
	private final String SITE_LIST_URL_STR = "http://dbpubs.stanford.edu:8091/%7Etestbed/doc2/WebBase/crawl_lists/crawled.122010";
	private URL SITE_LIST_URL = null;

	private final int NUM_OUTPUT_COLUMNS = 5;
	private final int CONTENT_COL_INDEX = 5;
	
	public static URL DISTRIBUTOR_DEMON_URL = null;
	
	// Keys for property configurations:
	public static final String WEBBASE_DISTRIBUTOR_DEMON_URL = "wbDistribDemonUrlStr";
	public static final String WEBBASE_CRAWL_NAME = "wbCrawlName";
	public static final String WEBBASE_CRAWL_TYPE = "wbCrawlMime";
	public static final String WEBBASE_SITE_LIST_URL = "siteListURL";
	
    // Vector with true wbRecordReader each position that corresponds to a
    // field that this loader is to include wbRecordReader its return tuples: 
    private boolean[] mRequiredColumns = null;
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private int numColsToReturn = 5; // URL, Date, Position wbRecordReader site, docID within the delivery. 
    private boolean mRequiredColumnsInitialized = false;
    private WbRecord wbRec = null;

    private String signature;
    
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
    private String crawlName = null;
    private String crawlType = null;
    
    protected WbRecordReader wbRecordReader = null;
    
	/*-----------------------------------------------------
	| Constructors 
	------------------------*/
    
    public WebBaseLoader() {
    	try {
    		DISTRIBUTOR_DEMON_URL = new URL(DISTRIBUTOR_DEMON_URL_STR);
    	} catch (Exception e) {
    		logger.error("Badly formatted distributor demon URL: " + DISTRIBUTOR_DEMON_URL_STR);
    		return;
    	}
    	try {
    		SITE_LIST_URL = new URL(SITE_LIST_URL_STR);
    	}  catch (Exception e) {
    		logger.error("Badly formatted site list URL: " + SITE_LIST_URL_STR);
    	}
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
    
    public String relativeToAbsolutePath(String location, Path curDir) 
    throws IOException {      
    	String[] crawlAndType = location.split(":");
    	if (crawlAndType.length != 2) {
    		logger.error("Badly formatted crawl name/type string. Should be <name>:<type>. Was " + location);
    		return "";
    	}
    	crawlName = crawlAndType[0].trim();
    	crawlType = crawlAndType[1].trim();
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
	 * Location is expected to be a string /<crawlName>:<mimeType>, which is 
	 * returned from our relativeToAbsolutePath() method (which is called earlier)
	 */
	
	@Override
	public void setLocation(String location, Job theJob) throws IOException {
		
		job = theJob;
		
		// Strip off the silly '/', which we had to add in relativeToAbsolutePath():
		if (location.startsWith("/"))
			location = location.substring(1);
		
		String[] crawlSpec = location.split(":");
		if (crawlSpec.length != 2) {
			throw new IOException("WebBase crawl location must have the form <crawlName>:<crawlMime>. Instead, '" + 
								  location +
								  "' was passed wbRecordReader.");
		}
		crawlName = crawlSpec[0];
		crawlType= crawlSpec[1];
		if (!wbMimeTypes.contains(crawlType)) {
			throw new IOException("Specified WebBase crawl mime was '" +
						           crawlType +
						           "'. Must be one of " +
						           wbMimeTypes.toString());
								  
		}
		Configuration hadoopConf = job.getConfiguration();
		hadoopConf.set(WEBBASE_DISTRIBUTOR_DEMON_URL, DISTRIBUTOR_DEMON_URL.toExternalForm());
		hadoopConf.set(WEBBASE_SITE_LIST_URL, SITE_LIST_URL.toString());
		hadoopConf.set(WEBBASE_CRAWL_NAME, crawlName);
		hadoopConf.set(WEBBASE_CRAWL_TYPE, crawlType);
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
		return new WbInputFormat(job);
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
}
