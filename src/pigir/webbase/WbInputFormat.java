package pigir.webbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import pigir.pigudf.MultiTypeProperties;

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
	private static int MAX_SITE_LIST_LINE_LEN = 200; // Maximum number of bytes in a line in site list files.

	// Pattern to match the site list summary at the end of site list files:
	private static final Pattern SITE_LIST_SUMMARY_LINE_Pattern = Pattern.compile("[\\d]* pages in repository.*");

	
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
	 * @param jobwbJobProperties
	 * @throws IOException
	 */
	public WbInputFormat(MultiTypeProperties wbJobProperties) throws IOException {
	
		// TODO: Check whether WbInputFormat constructor is called multiple times, and only compute slices once.

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
								  " (" +
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
		
		// Go through the site list and build a data structure that holds the wanted sites:
		//    ArrayList<SiteListEntry>: <siteName>,<numPagesThatSite>,<accumulatedPagesInclThatSite>
		
		// First, if user specified a 'firstSite', skip all entries up to
		// that site:
		boolean foundIt = false;
		if (!distributorContact.startSite.isEmpty()) {
			siteListReader.mark(MAX_SITE_LIST_LINE_LEN);
			while ((siteAndNumPages = siteListReader.readLine()) != null) {
				if (siteAndNumPages.startsWith(distributorContact.startSite)) {
					// Have code below re-read this entry in the site list:
					siteListReader.reset();
					foundIt = true;
					break;
				}
				siteListReader.mark(MAX_SITE_LIST_LINE_LEN);
			}
			// Did not find the startSite entry. Complain:
			if (!foundIt) {
				String errMsg = "LOAD command called for '" + distributorContact.startSite + "' to be the first site to retrieve. That site does not exist in this crawl " +
				"(" + distributorContact.getCrawlName() +
				" at " + distributorContact.getDistributorMachineName() + ":" + distributorContact.getDistributorPort() + ".";
				logger.error(errMsg);
				throw new IOException(errMsg);
			}
		}
		
		while ((siteAndNumPages = siteListReader.readLine()) != null) {
			// Get a [<siteName>, <numPages>]:
			siteNumPagePairs = siteAndNumPages.split("[\\s]");
			if (siteNumPagePairs.length != 2) {
				// This is (usually) the final line in the site list file that
				// summarizes how many pages there are:
				// E.g. "70918411 pages in repository, 31777 sites of 36181 were found for mimetype text'."
				if (SITE_LIST_SUMMARY_LINE_Pattern.matcher(siteAndNumPages).matches())
					continue;
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
			// Some sites in the site list have zero pages. Don't 
			// create a sitelist entry:
			if (numPagesThisSite > 0)
				siteListEntries.add(new SiteListEntry(siteName, numPagesThisSite, totalNumPages, ++crawlPos));
			// If user's LOAD command specified an end site, check
			// whether this one was it:
			if (!distributorContact.endSite.isEmpty() && siteName.equalsIgnoreCase(distributorContact.endSite))
				break;
			// If we already have the number of pages we need, quit:
			if ((distributorContact.getNumPagesWanted() != Constants.ALL_PAGES_WANTED) && 
				(distributorContact.getNumPagesWanted() <= totalNumPages)) {
				break;
			}
		}
		if (numOfMappers <= 0)
			throw new IOException("Number of mappers must be at least 1. Specified instead: " + numOfMappers);
		
		pagesPerMapper = distributorContact.getNumPagesWanted() / numOfMappers;
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
		int[] splitSeamIndices = new int[numMappers];
		int lastComputedSeamIndex = -1;
		
		// Find the indices into the site list that are seams
		// of splits. We'll get an array like [10,45,72]. This array
		// would be for four splits: 0-10, 11-45, 46-72, 73-end. Each
		// range of sites will contain just under the pagesPerMapper
		// number of pages:
		for (int mapperNum=0; mapperNum<numMappers; mapperNum++) {
			// Do a binary search over the part of the site list that
			// is still to be partitioned. The result will be the index
			// into the site list to the site that will keep the total
			// of this split to just below the optimal (pagesPerMapper)
			// However: we want to prevent sites with less than 3 pages 
			// from becoming their own split. Those are sites whose robots.txt 
			// keeps us out. The situation arises when the next site has more 
			// pages than the per-slice limit.
			seamIndex = indexIntoSiteEntryList(siteListEntries, first, upto, pageLimit);
			if ((lastComputedSeamIndex >= 0) &&
				(siteListEntries.size() > seamIndex) &&
				(seamIndex == splitSeamIndices[lastComputedSeamIndex] + 1) &&
				(siteListEntries.get(seamIndex)).numPages < 3) {
				// Keep looking for a site that's larger than just 2 pages:
				int newSeamIndex = seamIndex + 1;
				while ((newSeamIndex < siteListEntries.size()) &&
					   (siteListEntries.get(newSeamIndex)).numPages < 3) {
					newSeamIndex++;
				}
				seamIndex = newSeamIndex;
			}
			splitSeamIndices[mapperNum] = seamIndex;
			// Point to this last seam in the splitSeamIndices:
			lastComputedSeamIndex++;
			// If we are out of sites, then the sites we already have
			// in our list cover the total number of pages that the user
			// requested:
			if (siteListEntries.size() <= seamIndex)
				break;
			// Next total accumulated pages we will look for to identify a seam:
			pageLimit = siteListEntries.get(seamIndex).accumulatedPages + pagesPerMapper;
			first = seamIndex + 1;
		}
		
		// Now we have the list of split indices. Generate
		// WbInputSplit objects for each split. lastComputedSeamIndex
		// has a pt into the splitSeamIndices array to the last computed
		// seam. The remainder of that array is empty:

		SiteListEntry lowSite = siteListEntries.get(0);
		SiteListEntry highSite = null;
		int accumulatedNumPagesPrevSplit = 0;
		int numPagesCurrSplit = -1;
		for (int i=0; i<=lastComputedSeamIndex; i++) {
			if (i == lastComputedSeamIndex) {
				// No matter how many pages we absorbed into the
				// earlier splits, we must absorb all the rest into
				// this last one: 
				highSite = siteListEntries.get(siteListEntries.size() - 1);
				numPagesCurrSplit = highSite.accumulatedPages - accumulatedNumPagesPrevSplit;
			} else {
				highSite = siteListEntries.get(splitSeamIndices[i]);
				numPagesCurrSplit = highSite.accumulatedPages - accumulatedNumPagesPrevSplit;	//
			}
			WbInputSplit oneSplit = new WbInputSplit(lowSite.site, 
													 highSite.site,
													 numPagesCurrSplit,
													 distributorContact);
			splits.add(oneSplit);
			// The next site in the sitelist; the one after the current 
			// highSite:
			if (siteListEntries.size() > splitSeamIndices[i] + 1) {
				lowSite =  siteListEntries.get(splitSeamIndices[i] + 1);
				accumulatedNumPagesPrevSplit = highSite.accumulatedPages;
			} else
				break;
		}
		
		String logMsg = "Computed the following splits:";
		for (InputSplit oneSplit : splits) {
			logMsg += "\n     " + (WbInputSplit) oneSplit;
		}
		logger.info(logMsg);		
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
		long largestSplit = 0;
		for (InputSplit split : (List<InputSplit>) splits) {
			if (split.getLength() > largestSplit)
				largestSplit = split.getLength();
		}				
		context.getConfiguration().setLong("pig.maxCombinedSplitSize", largestSplit);
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
