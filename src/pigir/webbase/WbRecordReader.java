package pigir.webbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import pigir.webbase.wbpull.CrawlEntry;
import pigir.webbase.wbpull.crawlFinder.CrawlFinder;
import pigir.webbase.wbpull.webStream.WebStreamIterator;
import pigir.webbase.wbpull.webStream.WebStreamType;

public class WbRecordReader extends RecordReader<String, Text>{
	
	private static final boolean DO_READ_CONTENT = true;
	private WbRecord valueWbRecord = null;
	private String crawlName;
	private String crawlType;
	
	private WebStreamIterator webBaseStream;
	
	private Logger logger;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		logger = Logger.getLogger(WebBaseLoader.class.getName());
		PropertyConfigurator.configure("conf/log4j.properties");
	
		// Process arguments
		Configuration conf = context.getConfiguration();
		crawlName = conf.get(WebBaseLoader.WEBBASE_CRAWL_NAME);
		crawlType = conf.get(WebBaseLoader.WEBBASE_CRAWL_MIME);
		
		// Ensure that name and type arguments were supplied
		if(crawlName == null || crawlType == null) {
			logger.error("Either crawl name or crawl type not specified.");
			throw new IOException("Either WebBase crawl name or crawl type not specified.");
		}
		
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
		
		webBaseStream = (WebStreamIterator) CrawlFinder.GetWebStream(entry, WebStreamType.ITERATOR);
		
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
	
	@Override
	public String getCurrentKey() throws IOException,
			InterruptedException {
		return valueWbRecord.getMetadata().getURLAsString();
	}

	@Override
	public WbRecord getCurrentValue() throws IOException, InterruptedException {
		return valueWbRecord;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 100.0f * (((float) webBaseStream.getNumPagesRetrieved()) / ((float)webBaseStream.getTotalNumPages()));   
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	// TODO: fill wbRecordReader.
}
