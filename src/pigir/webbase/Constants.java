package pigir.webbase;

import java.net.InetAddress;
import java.net.UnknownHostException;


public final class Constants {
	
	//-------------------------------- Behavior-controlling Constants --------------------------
	
	// Indicator that all pages are wanted (for use in LOAD command):
	public static final int ALL_PAGES_WANTED = -1;
	
	// Number pages after which WebBaseLoader should log progress:
	public static final int WB_PAGE_LOAD_REPORT_GRANULARITY = 100;
	// Time to live for crawl directory cache in milliseconds. The 
	// 24 is the number of hours.
	public static final long CRAWL_DIR_LIST_TTL = 24 * 3600 * 1000;
	//****public static final int DEFAULT_NUM_OF_SPLITS = 2; // 40;
	public static final int DEFAULT_NUM_OF_SPLITS = 2;
	
	// Number of times to try contacting a distributor demon:
	public static final int NUM_DISTRIB_DEMON_ATTEMPTS = 4;
	// Distrib demon repeat attempts wait times in seconds:
	public static final int[] DISTRIB_DEMON_ATTEMPT_PAUSES = {4,6,8};
	
	// Number of times to try contacting a distributor:
	public static final int NUM_DISTRIB_ATTEMPTS = 4;
	// Distrib repeat attempts wait times in seconds. The first wait time
	// will always be honored to give the distributor time to fire up
	// after the distributor demon spawned it:
	public static final int[] DISTRIB_ATTEMPT_PAUSES = {6,6,10,10};
	
	
	
	//-------------------------------- WebBase Server Related Constants --------------------------
	
	public static final String WB_DOMAIN = ".stanford.edu";
	public static final String REMOTE_CRAWL_DIRECTORY_FILE_NAME = "crawlDirectory.txt";
	public static final String LOCAL_CRAWL_DIRECTORY_FILE_NAME = "resources/" + REMOTE_CRAWL_DIRECTORY_FILE_NAME;
	public static final String CRAWL_DIR_LIST_URL_STR =	
		"http://infolab.stanford.edu/~testbed/WBMetadata/" + REMOTE_CRAWL_DIRECTORY_FILE_NAME;
	public static final String CRAWL_SITE_LIST_URL_STUB =
		"http://infolab.stanford.edu/~testbed/WBMetadata/";
	
	// Array Index Constants:
	
	// Example: 2003-06,text,monthly,2003,06,00,WB1,7006,96060000,2003-06.tx,406,ddaemon.log.062003,2003-06
	public static final int CRAWL_DIR_CRAWL_NAME = 0;
	public static final int CRAWL_DIR_CRAWL_TYPE = CRAWL_DIR_CRAWL_NAME + 1;
	public static final int CRAWL_DIR_FREQUENCY = CRAWL_DIR_CRAWL_TYPE + 1;
	public static final int CRAWL_DIR_Year = CRAWL_DIR_FREQUENCY + 1;
	public static final int CRAWL_DIR_MONTH = CRAWL_DIR_Year + 1;
	public static final int CRAWL_DIR_DAY = CRAWL_DIR_MONTH + 1;
	public static final int CRAWL_DIR_DISTRIB_DEMON_MACHINE_NAME = CRAWL_DIR_DAY + 1;
	public static final int CRAWL_DIR_DISTRIB_DEMON_MACHINE_PORT = CRAWL_DIR_DISTRIB_DEMON_MACHINE_NAME + 1;
	public static final int CRAWL_DIR_NUM_PAGES = CRAWL_DIR_DISTRIB_DEMON_MACHINE_PORT + 1;
	public static final int CRAWL_DIR_SITELIST_FILENAME = CRAWL_DIR_NUM_PAGES + 1;

	
	//-------------------------------- Job Configuration Properties Keys --------------------------
	
	
	public static final String WB_DISTRIBUTOR_DEMON_KEY = "distribDemonKey";
	public static final String NUM_OF_CPUS_PROP_NAME = "mapred.map.tasks";
	
	//-------------------------------- Timeouts --------------------------
	
	
	public static final int WB_CONNECTING_TIMEOUT = 7000;     // Connecting to a Webbase port
	public static final int WB_INTERNET_READ_TIMEOUTE = 7000; // Reading from a Webbase port
	
	
	//-------------------------------- Constant-Returning Static Methods --------------------------	
	
	/*-----------------------------------------------------
	| getHostInfo() 
	------------------------*/
	
	/**
	 * @return <hostname/ipAddr> as a string.
	 */
	public static String getHostInfo() {
		try {
			return InetAddress.getLocalHost().getHostName() + "/" + 
				   InetAddress.getLocalHost().getHostAddress();
			
		} catch (UnknownHostException e) {
			return "";
		}
	}
}