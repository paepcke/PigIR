package pigir.webbase;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

import pigir.MultiTypeProperties;
import pigir.warc.WarcRecord;

public class WebBaseLoader extends LoadFunc implements LoadPushDown {

	public final Logger logger = Logger.getLogger(getClass().getName());
	
	private final String usage = "LOAD crawlName[colon-separated numPages, startSite, endSite]. Ex: LOAD 'foo:10' or LOAD 'foo:10:www.adobe.com' or LOAD 'foo:::zookeeper.com'";
	
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
    
    private CommandLineSpec loadCommand;
    private DistributorContact distributorContact = null;

    private WbInputFormat wbInputFormat = null;
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
            
            // Create separate columns from the WebBase header information,
            // followed by a bag of Pig Maps containing the HTTP header
            // fields that happened to be present in the Web page,
            // followed by the the content field.
            
            // See WbRecord.mandatoryHeaderFields for the mandatory
            // WebBase header fields we include in the following 'for' loop:

            int resFieldIndex = -1;
            for (String headerKey : wbRec.mandatoryKeysHeader()) {
            	// Check whether this WebBase header field is wanted
            	// in the result tuple (or is being projected out):
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
            
            // Now the bag of HTTP header fields:
            if ((mRequiredColumns != null) && (++resFieldIndex < numColsToReturn)  && mRequiredColumns[resFieldIndex]) { 
            	mProtoTuple.add(wbRec.get(WbRecord.HTTP_HEADER_MAP));
            }
            // Check whether the WebBase record content is wanted wbRecordReader the 
            // result tuple (or is being projected out):
            if ((mRequiredColumns != null) && (++resFieldIndex < numColsToReturn)  && mRequiredColumns[resFieldIndex]) { 
            	mProtoTuple.add(wbRec.get(WbRecord.CONTENT));
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
    	
		loadCommand = new CommandLineSpec(location);
		// Set the job configuration to know that the proper
		// InputFormat subclass to use is our WbInputFormat.class.
		// Else the default PigInputFormat is used, which assumes
		// files as input.
		theJob.setInputFormatClass(WbInputFormat.class);
		
    	distributorContact = DistributorContact.getCrawlDistributorContact(loadCommand.crawlName, loadCommand.numPagesWanted, loadCommand.startSite, loadCommand.endSite);
    	if (distributorContact == null) {
    		String errMsg = "No distributor found for crawl named '" + loadCommand.crawlName + "'. Double check the name and spelling.";
    		logger.error(errMsg);
    		throw new IOException(errMsg);
    	}
		int numReducers = theJob.getNumReduceTasks();
		logger.info("Number of reduce tasks from Job object is " + numReducers);
		if (numReducers <= 0)
			wbJobProperties.setInt(Constants.NUM_OF_CPUS_PROP_NAME, Constants.DEFAULT_NUM_OF_SPLITS);
		else
			wbJobProperties.setInt(Constants.NUM_OF_CPUS_PROP_NAME, theJob.getNumReduceTasks());

		logger.info("Setting number of slices to " + wbJobProperties.getInt(Constants.NUM_OF_CPUS_PROP_NAME, -1));
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
		if (wbInputFormat == null)
			wbInputFormat = new WbInputFormat(wbJobProperties);
		return wbInputFormat;
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
	
	// ------------------------------  Class CommandLineSpec -------------------------
	
	protected class CommandLineSpec {
		public String crawlName;
		public int numPagesWanted = Constants.ALL_PAGES_WANTED;
		public String startSite = "";
		public String endSite = "";

	/*-----------------------------------------------------
	| Constructor 
	------------------------*/
		
		public CommandLineSpec (String loadCommandArg) throws IOException {
			parseCrawlSpec(loadCommandArg);
		}
		
	/*-----------------------------------------------------
	| parseCrawlSpec() 
	------------------------*/
	
	/**
	 * Given the argument to the Pig LOAD command, tease out the
	 * components, and initialize the relevant instance variables.
	 * Examples: LOAD '2003-06:2:www.hp.com:www.ssa.gov' AS ...
	 * 
	 * @param loadCommandArg <crawlName>[:<numPages>[:<startSite>[:<endSite]]].
	 * Default when <numPages> is absent: all.
	 * @throws IOException
	 */
	private void parseCrawlSpec(String loadCommandArg) throws IOException {
		
		String[] loadCommandPieces = loadCommandArg.split(":");
		
    	if (loadCommandPieces.length > 4) {
    		String errMsg = "Too many parameters to Pig LOAD command. Usage: " + usage + ". Was " + loadCommandArg; 
    		logger.error(errMsg);
    		throw new IOException(errMsg);
    	}
    	
    	crawlName = loadCommandPieces[0].trim();
    	
    	if (loadCommandPieces.length > 1) {
    		// Second parm needs to be a number or empty, indicating that all pages are wanted:
    		if (loadCommandPieces[1].isEmpty())
    			numPagesWanted = Constants.ALL_PAGES_WANTED;
    		else 
    			try {
    				numPagesWanted = Integer.parseInt(loadCommandPieces[1]);
    			} catch (NumberFormatException e) {
    				String errMsg = "Bad page number in Pig LOAD command. Usage: " + usage + ". Was " + loadCommandArg; 
    				logger.error(errMsg);
    				throw new IOException(errMsg);
    			}
    	}
    	
    	if (loadCommandPieces.length > 2) {
    		startSite = loadCommandPieces[2];
    	}
    	
    	if (loadCommandPieces.length > 3) {
    		endSite = loadCommandPieces[3];
    	}
	}
			
	public String toString() {
		return "LoadCommand<" + crawlName + ":" + numColsToReturn + ":" + startSite + ":" + endSite + ">";
	}
	
	}
}
