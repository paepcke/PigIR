package pigir.webbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import pigir.MultiTypeProperties;

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

	DistributorContact distributorContact = null;
	BufferedReader siteListReader = null;
	
	int numOfMappers;
	
	Logger logger = null;
	
	List<InputSplit> splits = new ArrayList<InputSplit>();
	
	/*-----------------------------------------------------
	| Constructor
	------------------------*/
	/**
	 * Gather necessary information, then compute split distribution.
	 * @param jobContext
	 * @throws IOException
	 */
	public WbInputFormat(MultiTypeProperties wbJobProperties) throws IOException {
		
		if (wbJobProperties == null)
			throw new IOException("Bad WebBase configuration: " + wbJobProperties);
		
		logger = Logger.getLogger(getClass().getName());
		// PropertyConfigurator.configure("conf/log4j.properties");
		
		// Get URL of distributor demon from job configuration, and
		// get a distributor:
		
		String distributorContactStr = wbJobProperties.getProperty(Constants.WB_DISTRIBUTOR_DEMON_KEY); 
		distributorContact = new DistributorContact(distributorContactStr);
		
		String siteListURLStr= null;
		try {
			File siteListPath = new File(distributorContact.getSiteListPath());
			String siteListFileName = siteListPath.getName();
			siteListURLStr = Constants.CRAWL_SITE_LIST_URL_STUB + siteListFileName; 
			URL siteListURL = new URL(siteListURLStr);
			siteListReader = new BufferedReader(new InputStreamReader(siteListURL.openStream()));
		} catch (IOException e) {
			throw new IOException("Error retrieving site list from " +
								  siteListURLStr +
								  ". (" +
								  e.getMessage() +
								  ").");
		}
		
		// Get max number of map tasks (defaulting to DEFAULT_NUM_OF_SPLITS) if needed.
		numOfMappers = wbJobProperties.getInt(Constants.NUM_OF_CPUS_PROP_NAME, Constants.DEFAULT_NUM_OF_SPLITS);
		if (numOfMappers <= 0)
			numOfMappers = 1;
		
		splits = computeSplitBoundaries();
	}
	
	/*-----------------------------------------------------
	| computeSplitBoundaries()
	------------------------*/
	
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
		String siteName;
		int numPagesThisSite;
		int totalNumPages = 0;
		int badSiteListLine = 0;
		
		int pagesPerMapper;
		int crawlPos = -1;
		
		ArrayList<SiteListEntry> siteListEntries = new ArrayList<SiteListEntry>(); 
		
		// Go through the site list and build two data structures:
		//    ArrayList<SiteListEntry>: <siteName>,<numPagesThatSite>,<accumulatedPagesInclThatSite>
		//    ArrayList<SiteIndexEntry: <accumulatedPages>,<pt into site entry list>
		while ((siteAndNumPages = siteListReader.readLine()) != null) {
			// Get a [<siteName>, <numPages>]:
			siteNumPagePairs = siteAndNumPages.split("[\\s]");
			if (siteNumPagePairs.length != 2) {
				logger.warn("Bad site list entry (more or fewer than two tokens): '" + siteAndNumPages + "'.");
				if (++badSiteListLine > MAX_NUM_BAD_SITELIST_ENTRIES) {
					throw new IOException(makeSiteListErrorMsg(siteAndNumPages));
				} else
					continue;
			}
			siteName = siteNumPagePairs[SITE].trim();
			// Skip over comments:
			if (siteName.startsWith("#")) {
				continue;
			}
			try {
				numPagesThisSite = Integer.parseInt(siteNumPagePairs[NUM_PAGES]);
			} catch (NumberFormatException e) {
				logger.warn("Bad site list entry (second token is not an integer): '" + siteAndNumPages + "'.");
				if (++badSiteListLine > MAX_NUM_BAD_SITELIST_ENTRIES) {
				throw new IOException(makeSiteListErrorMsg(siteAndNumPages) + "(Second token is not an integer).");
				} else
					continue;
			}
			totalNumPages += numPagesThisSite;
			siteListEntries.add(new SiteListEntry(siteName, numPagesThisSite, totalNumPages, ++crawlPos));
			
		}
		
		if (numOfMappers <= 0)
			throw new IOException("Number of mappers must be at least 1. Specified instead: " + numOfMappers);
		
		pagesPerMapper = totalNumPages / numOfMappers;
		return generateSplits(pagesPerMapper, numOfMappers, siteListEntries);
	}
	
	/*-----------------------------------------------------
	| generateSplits() 
	------------------------*/
	
	/**
	 * Given a list of crawl site list entries, partition the list 
	 * into roughly equal-size sets of sites.  Return a list of WbInputSplit
	 * objects for use by Hadoop.
	 * @param pagesPerMapper The ideal number of pages in each split. 
	 * @param numMappers The number of mapper tasks
	 * @param siteListEntries ArrayList of objects that contain information 
	 * about each Web site in the crawl.
	 * @return A list of WbInputSplit objects.
	 */
	private List<InputSplit> generateSplits(int pagesPerMapper, 
			                                int numMappers,
			                                ArrayList<SiteListEntry> siteListEntries) {
		int first = 0;
		int upto  = siteListEntries.size();
		int pageLimit = pagesPerMapper;
		int seamIndex;
		int prevSeam = -1;
		int[] splitSeamIndices = new int[numMappers];
		
		// Find the indices into the site list that are seams
		// of splits. We'll get an array like [10,45,72]. This array
		// would be for four splits: 0-10, 11-45, 46-72, 73-end. Each
		// range of sites will contain just under the pagesPerMapper
		// number of pages:
		for (int mapperNum=0; mapperNum<numMappers; mapperNum++) {
			// Do a binary search over the part of the site list that
			// is still to be partitioned. The result will be the index
			// into the site list to the site that will keep the total
			// of this split to just below the optimal (pagesPerMapper):
			seamIndex = indexIntoSiteEntryList(siteListEntries, first, upto, pageLimit);
			splitSeamIndices[mapperNum] = seamIndex;
			pageLimit = siteListEntries.get(seamIndex).accumulatedPages + pagesPerMapper;
			first = seamIndex + 1;
		}
		
		// Now we have the list of split indices. Generate
		// WbInputSplit objects for each split:
		int lastIndex = splitSeamIndices[splitSeamIndices.length - 1];
		SiteListEntry lowSite = null;
		SiteListEntry highSite = null;
		int numPagesPrevSplit = 0;
		int numPagesCurrSplit = 0;
		for (int currentSeam : splitSeamIndices) {
			lowSite  = siteListEntries.get(prevSeam + 1);
			highSite = siteListEntries.get(currentSeam < lastIndex ? currentSeam : siteListEntries.size() - 1);
			numPagesCurrSplit = highSite.accumulatedPages - numPagesPrevSplit;
			WbInputSplit oneSplit = new WbInputSplit(lowSite.site, 
													 highSite.site,
													 numPagesCurrSplit,
													 distributorContact);
			splits.add(oneSplit);
			numPagesPrevSplit = numPagesCurrSplit;
			prevSeam = currentSeam;
		}
		
		return splits;
	}
	
	/*-----------------------------------------------------
	| indexIntoSiteEntryList() 
	------------------------*/
	
	/**
	 * Given the complete site entry list, two indices into
	 * the list to denote a slice of the list, and a target
	 * number of pages, find the index to the entry whose cumulative 
	 * number of pages is just below the given target.
	 * 
	 * Uses efficient binary search, since the lists contain tens
	 * of thousands of entries.
	 * @param siteList
	 * @param first
	 * @param upto
	 * @param numPagesWanted
	 * @return An integer that is the index into siteList to the entry.
	 */
	private int indexIntoSiteEntryList (ArrayList<SiteListEntry> siteList,
										int first,
										int upto,
										int numPagesWanted) {
		int mid = -1;
		while (first < upto) {
			mid = (first + upto) / 2;  // Compute mid point.
			if (numPagesWanted < siteList.get(mid).accumulatedPages) {
				upto = mid;     // repeat search in bottom half.
			} else if (numPagesWanted > siteList.get(mid).accumulatedPages) {
				first = mid + 1;  // Repeat search in top half.
			} else {
				break;     // Found it. return position
			}
		}
		if (mid >= 0)
			return mid;
		else
			return upto;
	}
	
	/*-----------------------------------------------------
	| makeSiteListErrorMsg()
	------------------------*/

	private String makeSiteListErrorMsg (String siteListLine) {
		return "Sitelist at " +
			    distributorContact.getSiteListPath() + 
			    " has too many bad lines" +
			    "\n (more than " +
			    MAX_NUM_BAD_SITELIST_ENTRIES +
			    "). Most recently: \n'" +
			    siteListLine +
			    "'.";		
	}

	// ---------------------------------------  Methods Required from  Superclass ---------------------------
	
	/*-----------------------------------------------------
	| createRecordReader()
	------------------------*/
	@Override
	public RecordReader<WbInputSplit, Text>
	createRecordReader(InputSplit split, 
			TaskAttemptContext context) {
		return new WbRecordReader();
	}

	/*-----------------------------------------------------
	| getSplits()
	------------------------*/
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
	InterruptedException {
		return splits;
	}

	// ---------------------------------------  Support Classes ---------------------------	
	
	/**
	 * One entry in the list of sites in one crawl (see head comment above)
	 * @author paepcke
	 *
	 */
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
		
		//int TEST_NUM_OF_MAPPERS = 10;
		//int TEST_NUM_OF_MAPPERS = 1;
		int TEST_NUM_OF_MAPPERS = 0;
		
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
			System.out.println(split.getStartSite() + ", " + split.getEndSite() + " (" + split.getNumPages() + ")");
		}
		
	}
 }
