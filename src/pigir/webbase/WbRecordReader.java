package pigir.webbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import pigir.webbase.wbpull.CrawlEntry;
import pigir.webbase.wbpull.crawlFinder.CrawlFinder;
import pigir.webbase.wbpull.webStream.BufferedWebStreamIterator;

/**
 * Record reader for stream from WebBase. Splits are objects containing
 * a start site, and end site, and the URL of the distributor demon that
 * will provide a distributor to use. The split object also contains the
 * total number of Web pages in this split.
 * 
 * The task here is to contact the distributor demon, get a distributor,
 * and then read page by page from the stream. Stream reading is done by
 * a BufferedWebStreamIterator, which reads some number of pages at a
 * time into a buffer to ammortize communication cost with WebBase.
 *  
 * @author paepcke
 *
 */
public class WbRecordReader extends RecordReader<WbInputSplit, Text>{
	
	private static final boolean DO_READ_CONTENT = true;
	private WbRecord valueWbRecord = null;
	private String crawlName;
	private String crawlType;
	
	private BufferedWebStreamIterator webBaseStream;
	
	private Logger logger;
	private WbInputSplit mySplit = null;
	
	/*-----------------------------------------------------
	| initialize()
	------------------------*/
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		logger = Logger.getLogger(WebBaseLoader.class.getName());
		//PropertyConfigurator.configure("conf/log4j.properties");
	
		mySplit = (WbInputSplit) split;
		// Process arguments
		Configuration conf = context.getConfiguration();
		crawlName = conf.get(WebBaseLoader.WEBBASE_CRAWL_NAME);
		crawlType = conf.get(WebBaseLoader.WEBBASE_CRAWL_TYPE);
		
		// Ensure that name and type arguments were supplied
		if(crawlName == null || crawlType == null) {
			logger.error("Either crawl name or crawl type not specified.");
			throw new IOException("Either WebBase crawl name or crawl type not specified.");
		}
		
		// Get a distributor for this split:
		logger.info("Getting distributor address for " + crawlName + ":" + crawlType);
		CrawlEntry entry = CrawlFinder.FindCrawl(crawlName, crawlType);
		if(entry == null) {
			String errMsg = "Crawl access information for '" + 
						  crawlName + ":" + crawlType + 
						  "' could not be obtained from WebBase distributor demon."; 
			logger.error(errMsg);
			throw new IOException(errMsg);
		}
		
		logger.info("Attempt to start WebBase stream from distributor " + 
				entry.getMachineName() + ".stanford.edu:" + entry.getPort());
		
		webBaseStream = new BufferedWebStreamIterator(entry.getMachineName(), 
													  entry.getPort(), 
													  mySplit.getStartSite(), 
													  mySplit.getEndSite(), 
													  mySplit.getNumPages());
		
		if (webBaseStream == null) {
			String errMsg = "Web page stream is null. Maybe crawl not found?"; 
			logger.error(errMsg);
			throw new IOException(errMsg);
		}
		
		logger.info("Successfully obtained WebBase stream " + 
					webBaseStream.getIp() + ":" + webBaseStream.getPort());
		
		/*for(int i = 0; i < 2; i++) {
			if(webBaseStream.hasNext())
				System.out.println(webBaseStream.next());
		}*/
	}

	/*-----------------------------------------------------
	| nextKeyValue()
	------------------------*/
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// Don't need to pull anything. Just check whether another
		// page is available:
		return nextKeyValue(DO_READ_CONTENT);
	}
	
	public boolean nextKeyValue(boolean readContents) throws IOException, InterruptedException {
		if (!webBaseStream.hasNext())
			return false;
		valueWbRecord = webBaseStream.next();
		return true;
	}
	
	/*-----------------------------------------------------
	| getCurrentKey()
	------------------------*/
	@Override
	public WbInputSplit getCurrentKey() throws IOException,
			InterruptedException {
		return mySplit;
	}

	/*-----------------------------------------------------
	| getCurrentValue()
	------------------------*/
	@Override
	public WbRecord getCurrentValue() throws IOException, InterruptedException {
		return valueWbRecord;
	}

	/*-----------------------------------------------------
	| getProgress()
	------------------------*/
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 100.0f * (((float) webBaseStream.getNumPagesRetrieved()) / ((float)webBaseStream.getTotalNumPages()));   
	}

	/*-----------------------------------------------------
	| close()
	------------------------*/
	@Override
	public void close() throws IOException {
		webBaseStream.close();
	}
}
