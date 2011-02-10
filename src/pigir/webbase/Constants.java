package pigir.webbase;


public final class Constants {
	
	// Indicator that all pages are wanted (for use in LOAD command):
	public static final int ALL_PAGES_WANTED = -1;
	
	// Number pages after which WebBaseLoader should log progress:
	public static final int WB_PAGE_LOAD_REPORT_GRANULARITY = 100;
	
	public static final String WB_DOMAIN = ".stanford.edu";
	public static final String CRAWL_DIRECTORY_LIST_FILE_NAME = "resources/crawl.directory.list";
	// Time to live for crawl directory cache in milliseconds. The 
	// 24 is the number of hours.
	public static final long CRAWL_DIR_LIST_TTL = 24 * 3600 * 1000;
	public static final int DEFAULT_NUM_OF_SPLITS = 1;
	public static final String CRAWL_DIR_LIST_URL_STR =	
		"http://infolab.stanford.edu/~testbed/WBMetadata/crawl.directory.list";
	public static final String CRAWL_SITE_LIST_URL_STUB =
		"http://infolab.stanford.edu/~testbed/WBMetadata/";
	
	// Job configuration keys:
	public static final String WB_DISTRIBUTOR_DEMON_KEY = "distribDemonKey";
	public static final String NUM_OF_CPUS_PROP_NAME = "mapred.map.tasks";
	
	// Timeouts:
	
	public static final int WB_CONNECTING_TIMEOUT = 7000;     // Connecting to a Webbase port
	public static final int WB_INTERNET_READ_TIMEOUTE = 7000; // Reading from a Webbase port
}