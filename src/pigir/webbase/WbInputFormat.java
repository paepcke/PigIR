package pigir.webbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * InputFormat for WebBase crawls. Mappers draw Web page streams
 * directly from WebBase. One overall job pulls one entire crawl.
 * Each split pulls a subset of that crawl's sites. 
 * 
 * The main task is to compute splits.
 * We do this using each crawl's site list. Those lists are 
 * available on the Web, and we pull them from there. Site 
 * lists are of the form:
 * 
 *     <siteName1>   <numOfPages1>
 *     <siteName2>   <numOfPages2>
 *     <siteName3>   <numOfPages3>
 *          ...
 * We pull in these lines, and add up the number of pages.
 * We then divide this total number of pages by the number
 * of processors we want involved in mappers. The result is
 * a split size. Note that the granularity for split boundaries
 * is a Web site (not a page).
 * 
 * Since sites have widely varying numbers of pages, we can
 * only approximate making all splits equal size. This of course
 * in addition to the sizes of Web pages varying.
 * 
 * 
 * @author paepcke
 *
 */
public class WbInputFormat extends InputFormat<WbInputSplit,Text> {
	
	private static int MAX_NUM_BAD_SITELIST_ENTRIES = 100;
	private static int DEFAULT_NUM_OF_SPLITS = 1;
	private static String NUM_OF_CPUS_PROP_NAME = "mapred.map.tasks";
	private final int SITE_INDEX_GRANULARITY = 10;

	String distribDemonUrlStr = "";
	URL    distribDemonUrl = null;
	
	String siteListUrlStr = "";
	URL    siteListUrl = null;
	BufferedReader siteListReader = null;
	
	int numOfMappers;
	
	//Vector<WbInputSplit> splits = new Vector<WbInputSplit>();
	List<InputSplit> splits = new ArrayList<InputSplit>();
	
	/**
	 * Gather necessary information, then compute split distribution.
	 * @param jobContext
	 * @throws IOException
	 */
	public WbInputFormat(Job job) throws IOException {
		
		Configuration conf = job.getConfiguration();
		
		// Get URL of distributor demon from job configuration, and
		// get a distributor:
		distribDemonUrlStr = conf.get(WebBaseLoader.WEBBASE_DISTRIBUTOR_DEMON_URL_STRING);
		try {
			distribDemonUrl = new URL(distribDemonUrlStr);
		} catch (MalformedURLException e) {
			throw new IOException("Could not obtain URL of WebBase distributor demon from job context parameter. URL from jobContext was: '" + 
								  distribDemonUrlStr +
								  "'.");
		}
		
		// Get URL of desired crawl's site list from jobContext:
		
		try {
			siteListReader = new BufferedReader(new InputStreamReader(siteListUrl.openStream()));
		} catch (IOException e) {
			throw new IOException("Error retrieving site list from " +
								  distribDemonUrlStr +
								  ". (" +
								  e.getMessage() +
								  ").");
		}
		
		// Get max number of map tasks (defaulting to DEFAULT_NUM_OF_SPLITS) if needed.
		numOfMappers = conf.getInt(NUM_OF_CPUS_PROP_NAME, DEFAULT_NUM_OF_SPLITS);
		
		splits = computeSplitBoundaries();
	}
	
	/**
	 * Read the desired crawl's site list from WebBase site, and compute the
	 * number of splits. Strategy:
	 *   o Collect all site/numPages pairs from the site list
	 *   o In an ArrayList accumulate total number of pages as we go.
	 *   o Also keep an index of pointers into the ArrayList that will at the
	 *     end allow us to quickly find the first and last site for each split.
	 *     
	 * Since crawls can contain tens of thousands of sites, we build an index
	 * as we go to make split boundary finding easier. The index maps a total number
	 * of pages to a pointer into list site entry list. We make an entry every
	 * SITE_INDEX_GRANULARITY'th site list entry. The index looks like this:
	 * 
	 *     <cumulativeNumOfPages> --> <ptIntoSiteList>
	 * 
	 * @throws IOException
	 */
	private List<InputSplit> computeSplitBoundaries() throws IOException {

		final short SITE = 0;
		final short NUM_PAGES = 1; 
		
		String siteAndNumPages;
		String[] siteNumPagePairs;
		int numPagesThisSite;
		int totalNumPages = 0;
		int badSiteListLine = 0;
		int numLinesRead = 0;
		
		int pagesPerMapper;
		int crawlPos = -1;
		
		ArrayList<SiteListEntry> siteListEntries = new ArrayList<SiteListEntry>(); 
		ArrayList<SiteIndexEntry> siteListIndex  = new ArrayList<SiteIndexEntry>(); 
		
		// Go through the site list and build two data structures:
		//    ArrayList<SiteListEntry>: <siteName>,<numPagesThatSite>,<accumulatedPagesInclThatSite>
		//    ArrayList<SiteIndexEntry: <accumulatedPages>,<pt into site entry list>
		while ((siteAndNumPages = siteListReader.readLine()) != null) {
			// Get a [<siteName>, <numPages>]:
			siteNumPagePairs = siteAndNumPages.split("[\\s]");
			if (siteNumPagePairs.length != 2) {
				if (++badSiteListLine > MAX_NUM_BAD_SITELIST_ENTRIES) {
					throw new IOException(makeSiteListErrorMsg(siteAndNumPages));
				}
			}
			try {
				numPagesThisSite = Integer.parseInt(siteNumPagePairs[NUM_PAGES]);
			} catch (NumberFormatException e) {
				throw new IOException(makeSiteListErrorMsg(siteAndNumPages) + "(Second token is not an integer).");
			}
			totalNumPages += numPagesThisSite;
			siteListEntries.add(new SiteListEntry(siteNumPagePairs[SITE], numPagesThisSite, totalNumPages, ++crawlPos));
			
			// If appropriate, make an index entry:
			if (++numLinesRead > SITE_INDEX_GRANULARITY) {
				siteListIndex.add(new SiteIndexEntry(totalNumPages, siteListEntries.size() - 1));
				numLinesRead = 0;
			}
		}
		
		pagesPerMapper = totalNumPages / numOfMappers;
		return generateSplits(pagesPerMapper, siteListEntries, siteListIndex);
	}
	
	/**
	 * Find one split boundary after another, advancing through
	 * the site list. Generate a split object for each split.
	 * @param pagesPerMapper
	 * @param siteListEntries
	 * @param siteListIndex
	 * @return
	 */
	private List<InputSplit> generateSplits(int pagesPerMapper, 
												ArrayList<SiteListEntry> siteListEntries,
												ArrayList<SiteIndexEntry> siteListIndex) {
		int thisSplitSize = 0;
		int highSiteIndex;
		SiteListEntry lowSite = siteListEntries.get(0);
		SiteListEntry highSite = null;
		int maxAccumulatedPages = pagesPerMapper;
		int prevAccumulatedPages = 0;
		
		for (int i=0; i<numOfMappers; i++) {
			highSite = findSplitBoundary(maxAccumulatedPages, siteListEntries, siteListIndex);
			highSiteIndex = highSite.crawlPosition;
			thisSplitSize = highSite.accumulatedPages - prevAccumulatedPages;
			prevAccumulatedPages = highSite.accumulatedPages;
			// Next upper split boundary:
			maxAccumulatedPages += thisSplitSize;
			splits.add(new WbInputSplit(lowSite.site, highSite.site, thisSplitSize, distribDemonUrl));
			lowSite = siteListEntries.get(Math.min(highSiteIndex + 1, siteListEntries.size() - 1));
			if (lowSite == highSite)
				break;
		}
		
		return splits;
	}
	
	/**
	 * Heavy lifting for finding split boundaries in site lists. 
	 * @param numOfAccumulatedPages Top number of pages that are contained in 
	 * all splits up to, and including the one that we are trying to find in
	 * this method.
	 * @param siteListEntries List of site list entry objects.
	 * @param siteListIndex   The index we build in computeSplitBoundaries().
	 * @return The sitelist entry that is the upper bound of the split.
	 */
	private SiteListEntry findSplitBoundary(int numOfAccumulatedPages,
			                      ArrayList<SiteListEntry> siteListEntries,
								  ArrayList<SiteIndexEntry> siteListIndex) {
		
		int halfPoint;
		int lowPoint = 0;
		int highPoint = siteListIndex.size() - 1;
		int lowBallSiteEntryPt;
		SiteIndexEntry indexEntry;
		
		while (true) {
			halfPoint = (highPoint - lowPoint) / 2;
			indexEntry = siteListIndex.get(lowPoint + halfPoint);
			if (indexEntry.totalPageAccumulation == numOfAccumulatedPages) {
				break;			}
			if (indexEntry.totalPageAccumulation > numOfAccumulatedPages) {
				highPoint = halfPoint;			
				if (highPoint <= lowPoint)
					break;
			} else {
				lowPoint += halfPoint + 1;
				if (lowPoint >= highPoint) {
					break;
				}
			}
		}
		if ((siteListIndex.get(lowPoint)).totalPageAccumulation > numOfAccumulatedPages)
			lowBallSiteEntryPt = siteListIndex.get(Math.max(0,lowPoint - 1)).siteEntryIndex;
		else
			lowBallSiteEntryPt = siteListIndex.get(lowPoint).siteEntryIndex;
		
		// Was the maximum split size reached within the first 
		// index granule?
		if (lowBallSiteEntryPt == SITE_INDEX_GRANULARITY)
			lowBallSiteEntryPt = 0;

		// We now know the index into the site entry list at which 
		// split size pages, or just below are contained. Since the
		// index only has entries every SITE_INDEX_GRANULARITY entries,
		// we now find the precise point in the site list where a split
		// boundary sits:
		
		SiteListEntry resListEntry;
		// If 
		for (int siteEntryPt=lowBallSiteEntryPt; 
			 siteEntryPt < Math.min(siteListEntries.size(), siteEntryPt + SITE_INDEX_GRANULARITY);
			 siteEntryPt++) {
			resListEntry = siteListEntries.get(siteEntryPt);
			if (resListEntry.accumulatedPages >= numOfAccumulatedPages)
				return resListEntry;
		}
		// Return the last entry:
		return siteListEntries.get(siteListEntries.size()-1);
	}
	
	
	
	private String makeSiteListErrorMsg (String siteListLine) {
		return "Sitelist at " +
			    distribDemonUrlStr +
			    " has too many bad lines (more than " +
			    MAX_NUM_BAD_SITELIST_ENTRIES +
			    "). Most recently: '" +
			    siteListLine +
			    "'.";		
	}

	// ---------------------------------------  Methods Required from  Superclass ---------------------------
	
	@Override
	public RecordReader<WbInputSplit, Text>
	createRecordReader(InputSplit split, 
			TaskAttemptContext context) {
		return new WbRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
	InterruptedException {
		return splits;
	}

	// ---------------------------------------  Support Classes ---------------------------	
	
	private class SiteIndexEntry {
		public int totalPageAccumulation;
		public int siteEntryIndex;
		
		public SiteIndexEntry(int theTotalPages, int theEntryIndex) {
			totalPageAccumulation = theTotalPages;
			siteEntryIndex = theEntryIndex;
		}
		
		public String toString() {
			return "SiteIndexEntry[Sofar: " + totalPageAccumulation + "; EntryIndex: " + siteEntryIndex + "]";
		}
	}

	private class SiteListEntry {
		public String site;            // Site name
		public int numPages;           // Num pages this site
		public int accumulatedPages;   // Pages in the crawl up to and including this site
		public int crawlPosition;      // Position of site within the crawl
		
		public SiteListEntry(String theSite, int theNumPages, int theAccumulatedPages, int theCrawlPos) {
			site = theSite;
			numPages = theNumPages;
			accumulatedPages = theAccumulatedPages;
			crawlPosition = theCrawlPos;
		}
		
		public String toString() {
			return "SiteListEntry[" + site + ":" + numPages + ":" + accumulatedPages + ":" + crawlPosition +  "]";
		}
	}

	// ---------------------------------------  Testing  -------------------		
	
	public WbInputFormat() throws IOException {
		
		int TEST_NUM_OF_MAPPERS = 10;
		// total num pages manually calculated: 3329
		String fakeSiteList = "" +
			   "www.calvoter.org	2751\n" +
			   "www.millerforcalifornia.com	32\n" +
			   "www.terminategray.com	2\n" +
			   "www.beattherecall.com	33\n" +
			   "www.brokennewz.com	2\n" +
			   "votearianna.com	3\n" +
			   "www.priceforgov.com	59\n" +
			   "mccarthy4gov.com	22\n" +
			   "selectsmart.com	52\n" +
			   "www.tacobell.com	2\n" +
			   "www.warrenfarrell.com	2\n" +
			   "www.rescuecalifornia.com	2\n" +
			   "www.whowantstobeagovernor.com	48\n" +
			   "www.devasbar.com	70\n" +
			   "www.bettercalifornia.org	2\n" +
			   "notocaliforniarecall.meetup.com	2\n" +
			   "www.sb-democracy.com	0\n" +
			   "www.the-gubernator.com	19\n" +
			   "www.governorken.org	61\n" +
			   "www.carsonforcalifornia.com	3\n" +
			   "www.smokersparty.com	129\n" +
			   "www.gowinters.com	33\n";
		
		
		//siteListReader = new BufferedReader(new InputStreamReader(siteListUrl.openStream()));
		siteListReader = new BufferedReader(new StringReader(fakeSiteList));
		
		// Get max number of map tasks (defaulting to DEFAULT_NUM_OF_SPLITS) if needed.
		numOfMappers = TEST_NUM_OF_MAPPERS;
		
		computeSplitBoundaries();
		System.out.println("Number of splits: " + splits.size());
		for (InputSplit oneSplit : splits) {
			WbInputSplit split = (WbInputSplit) oneSplit;
			System.out.println(split.startSite + ", " + split.endSite + " (" + split.numPages + ")");
		}
		
	}
	
	public static void main(String[] args) {
		try {
			new WbInputFormat();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
 }
