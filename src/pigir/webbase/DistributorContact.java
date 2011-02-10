package pigir.webbase;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import pigir.AsciiStringInputStream;
import pigir.AsciiStringOutputStream;

/**
 * Obtains and stores the contact information for one crawl's
 * distributor demon. 
 * 
 * To obtain a distributor demon contact, use 
 * DistributorContact.getCrawlDistributorAddr(crawlName, crawlType)
 * 
 * @author paepcke
 *
 */
public class DistributorContact implements Writable, Serializable {
	
	private static final long serialVersionUID = 8535254665314974009L;

	private static Logger logger = null;
	
	String distributorMachineName = null;
	int distributorPort = -1;
	String distributorDate = null;
	int numPages = -1;
	int numPagesWanted = Constants.ALL_PAGES_WANTED;
	String crawlName = null;
	String startSite = null;
	String endSite = null;
	String crawlType = null;
	String siteListPath = null;

	private static final HashMap<String,String> wbMimeTypes = new HashMap<String,String>() {
		private static final long serialVersionUID = 1L;
		{
    		put("au", "audio");
    		put("im", "image");
    		put("tx", "text");
    		put("un", "unknown");
    	}
    };
	
	/*-----------------------------------------------------
	| Constructors 
	------------------------*/

	public DistributorContact() {
		// Needed for creating a split on the worker nodes
		// via serialization. The readFields() method will 
		// be called during this process to initialize the
		// instance variables.
		
		logger = Logger.getLogger(WebBaseLoader.class);
	}
	
	public DistributorContact(String serializedContactObj) throws IOException {
		if (logger == null)
			logger = Logger.getLogger(WebBaseLoader.class);
		try {
			AsciiStringInputStream  recoveryStream = new AsciiStringInputStream(serializedContactObj);
			readObject(recoveryStream);
		} catch (Exception e) {
			String errMsg = "Cannot recover distributor contact object from job configuration." +
					"\n Serialized string is '" + serializedContactObj + "'.";
			logger.error(errMsg);
			throw new IOException(errMsg);
		}
	}
	
	public DistributorContact(
			String theDistributorMachineName, 
			int theDistributorPort,
			String theDate,
			int theNumPages,
			int theNumPagesWanted,
			String theStartSite,
			String theEndSite,
			String theCrawlName,
			String theCrawlType,
			String theSiteListPath) throws IOException {
		
		initVars(theDistributorMachineName, 
				theDistributorPort, 
				theDate,
				theNumPages,
				theNumPagesWanted,
				theStartSite,
				theEndSite,
				theCrawlName,
				theCrawlType,
				theSiteListPath);
	}

	public DistributorContact(
			String theDistributorMachineName, 
			String theDistributorPort,
			String theDate,
			int theNumPages,
			int theNumPagesWanted,			
			String theStartSite,
			String theEndSite,
			String theCrawlName,
			String theCrawlType,
			String theSiteListPath) throws IOException {

		initVars(theDistributorMachineName, 
				theDistributorPort,
				theDate,
				theNumPages,
				theNumPagesWanted,
				theStartSite,
				theEndSite,
				theCrawlName,
				theCrawlType,
				theSiteListPath);
	}

	/*-----------------------------------------------------
	| initVars() 
	------------------------*/
	
	// port as integer. Actual initialization happens here:
	private void initVars(
			String theDistributorMachineName, 
			int 	theDistributorPort,
			String	theDate,
			int		theNumPages,
			int  	theNumPagesWanted,
			String  theStartSite,
			String  theEndSite,
			String	theCrawlName,
			String	theCrawlType,
			String theSiteListPath) {

		logger = Logger.getLogger(WebBaseLoader.class);
		
		if (theDistributorMachineName.indexOf(Constants.WB_DOMAIN) > -1)
			distributorMachineName = theDistributorMachineName;
		else
			distributorMachineName = theDistributorMachineName + Constants.WB_DOMAIN;
		distributorPort = theDistributorPort;
		distributorPort = theDistributorPort;
		distributorDate = theDate;
		numPages = theNumPages;
		if (theNumPagesWanted < 0)
			numPagesWanted = theNumPages;
		else
			numPagesWanted = theNumPagesWanted;
		startSite = theStartSite;
		endSite   = theEndSite;
		crawlName = theCrawlName;
		crawlType = theCrawlType;
		siteListPath = theSiteListPath;
		
		if (logger == null)
			logger = WbRecordReader.getLogger();
	}

	// port as string:
	private void initVars(
			String theDistributorMachineName, 
			String theDistributorPort,
			String	theDate,
			int		theNumPages,
			int  	theNumPagesWanted,			
			String  theStartSite,
			String  theEndSite,
			String	theCrawlName,
			String	theCrawlType,
			String  theSiteListPath) throws IOException {
		int port;
		try {
			port = Integer.parseInt(theDistributorPort);
		} catch (Exception e) {
			String errMsg = "Bad crawl distributor machine port: " + theDistributorPort;
			logger.error(errMsg);
			throw new IOException(errMsg);
		}
		initVars(theDistributorMachineName,
				port,
				theDate,
				theNumPages,
				theNumPagesWanted,
				theStartSite,
				theEndSite,
				theCrawlName,
				theCrawlType,
				theSiteListPath);
	}
	
	/*-----------------------------------------------------
	| getCrawlDistributorContact() 
	------------------------*/
	
	/**
	 * Use, or refresh and then use the crawl directory list to find the contact
	 * information for one crawl's distributor demon.
	 *  
	 * @param crawlName
	 * @param crawlType
	 * @return a DistributorContact instance that holds all information necessary to 
	 * request a distributor. 
	 * @throws IOException
	 */
	public static DistributorContact getCrawlDistributorContact(String crawlName, int numPagesWanted, String startSite, String endSite) 
	throws IOException {
		
		File directoryListFile;
		
		directoryListFile = new File(Constants.CRAWL_DIRECTORY_LIST_FILE_NAME);
		if (!directoryListFile.exists() || isOld(directoryListFile))
			refreshDirectoryListFile();
		crawlName = crawlName.trim();

		// Derive the crawl type from the crawl name. The
		// element after the last '.' is a two-char code for
		// the type: tx for text, au for audio, etc (see WbMimeTypes above).
		File crawlNameFile = new File(crawlName);
		String crawlFileNamePart = crawlNameFile.getName();
		String[] crawlNameElements = crawlFileNamePart.split("\\.");
		if (crawlNameElements.length < 1) {
			String errMsg = "Bad crawl name. All crawl names have an extension '.xx' where 'xx' is a mime type code, like 'tx' or 'au'. This name was: " + crawlFileNamePart;
			logger.error(errMsg);
			throw new IOException(errMsg);
		}
		String mimeTypeCode = crawlNameElements[crawlNameElements.length - 1];
		String crawlMimeType = wbMimeTypes.get(mimeTypeCode);
		if (crawlMimeType == null)
			crawlMimeType = "text";
		
		try {
			FileInputStream fstream = new FileInputStream(Constants.CRAWL_DIRECTORY_LIST_FILE_NAME);
			BufferedReader buf = new BufferedReader(new InputStreamReader(fstream));
			String line;
			while((line = buf.readLine()) != null) {
				line = line.trim(); //trim any leading/trailing whitespace in the line
				//logger.debug("Crawl file line: " + line);
				// If the current line contains both the name and mime, then all good:
				if(line.indexOf(crawlName) != -1) {
					String[] crawlWords = line.split(" ");
					String machine = crawlWords[0];
					String port = crawlWords[1];
					String date = crawlWords[3];
					String numPagesStr = crawlWords[4];
					numPagesStr = numPagesStr.replaceAll("[^0-9]", "");
					int numPages;
					try {
						numPages = Integer.parseInt(numPagesStr);
					} catch (NumberFormatException e) {
						String errMsg = "Badly formatted number-of-pages spec found in crawl list directory:" +
								"\n'" + numPagesStr +
								"'. In entry '" +
								line +  "'.";
						logger.error(errMsg);
						throw new IOException(errMsg);
					}
					String siteListPath = crawlWords[5];
					return new DistributorContact(machine + Constants.WB_DOMAIN, // the .stanford.edu
												  port,
												  date,
												  numPages,
												  numPagesWanted,
												  startSite,
												  endSite,
												  crawlName,
												  crawlMimeType,
												  siteListPath);
				}
			}
			logger.debug("No distributor address found for " + crawlName);
			return null;
		}
		catch(IOException e) {
			logger.error("IO error encountered: " + e.getMessage());
			return null;
		}
	}
	
	
	/*-----------------------------------------------------
	| isOld() 
	------------------------*/
	
	/**
	 * Return true if the given file is older than CRAWL_DIR_LIST_TTL milliseconds.
	 * @param crawlListFile
	 * @return
	 */
	private static boolean isOld(File crawlListFile) {
		long lastModified = crawlListFile.lastModified();
		return ((System.currentTimeMillis() - lastModified) > Constants.CRAWL_DIR_LIST_TTL);
	}
	
	/*-----------------------------------------------------
	| refreshDirectoryListFile() 
	------------------------*/
	
	/**
	 * Retrieve a fresh crawl directory list from WebBase
	 * @throws IOException
	 */
	private static void refreshDirectoryListFile() throws IOException {
		// Is there an old copy around?
		File oldDirectoryListFile = new File(Constants.CRAWL_DIRECTORY_LIST_FILE_NAME);
		File tmpFile = new File(oldDirectoryListFile.getAbsolutePath() + ".tmp");
		URL crawlListURL = null;
		
		if (logger == null)
			logger = Logger.getLogger(WebBaseLoader.class);

		try {
			crawlListURL = new URL(Constants.CRAWL_DIR_LIST_URL_STR);
		} catch (MalformedURLException e) {
			// we checked the URL.
		}

		// If tmp file exists, get rid of it:
		FileUtils.deleteQuietly(tmpFile);
		
		// Copy the latest crawl directory list to the tmp file:
		try {
			FileUtils.copyURLToFile(crawlListURL, tmpFile, Constants.WB_CONNECTING_TIMEOUT, Constants.WB_INTERNET_READ_TIMEOUTE);
		} catch (Exception e) {
			String errMsg = "Could not copy fresh WebBase crawl directory list into cash:\n" +
							e.getMessage();
			if (oldDirectoryListFile.exists()) {
				errMsg += "\nUsing cached version.";
				logger.warn(errMsg);
				return;
			} else {
				errMsg += "\nNo previous version exists.";
				logger.error(errMsg);
				throw new IOException(errMsg);
			}
		}
		
		// Move the new copy to the live one:
		try {
			FileUtils.deleteQuietly(oldDirectoryListFile);
			FileUtils.moveFile(tmpFile, oldDirectoryListFile);
		} catch (Exception e) {
			String errMsg = "Could not update cached crawl directory list from (successfully downloaded) new copy.\n     " + e.getMessage();
			if (oldDirectoryListFile.exists()) {
				errMsg += "\n     Using cached version.";
				logger.warn(errMsg);
				return;
			} else {
				errMsg += "\nNo previous version exists.";
				logger.error(errMsg);
				throw new IOException(errMsg);
			}
		}
	};
	
	/*-----------------------------------------------------
	| writeObject() (for Serializable) 
	------------------------*/

	 private void writeObject(DataOutput out)
     throws IOException {
		 out.writeUTF(distributorMachineName);
		 out.writeInt(distributorPort);
		 out.writeUTF(distributorDate);
		 out.writeInt(numPages);
		 out.writeInt(numPagesWanted);
		 out.writeUTF(startSite);
		 out.writeUTF(endSite);
		 out.writeUTF(crawlName);
		 out.writeUTF(crawlType);
		 out.writeUTF(siteListPath);
	 }

	/*-----------------------------------------------------
	| write() (for Writable) 
	------------------------*/

	@Override
	public void write(DataOutput out) throws IOException {
		writeObject(out);
	}

	/*-----------------------------------------------------
	| readObject() (for Serializable) 
	------------------------*/

	 private void readObject(DataInput in)
     throws IOException, ClassNotFoundException {
		 distributorMachineName = in.readUTF();
		 distributorPort        = in.readInt();
		 distributorDate	    = in.readUTF();
		 numPages				= in.readInt();
		 numPagesWanted         = in.readInt();
		 startSite 				= in.readUTF();
		 endSite 				= in.readUTF();
		 crawlName				= in.readUTF();
		 crawlType				= in.readUTF();
		 siteListPath			= in.readUTF();
     }
	 
	/*-----------------------------------------------------
	| readFields() (For Writable) 
	------------------------*/
	 
	@Override
	public void readFields(DataInput in) throws IOException {
		try {
			readObject(in);
		} catch (ClassNotFoundException e) {
			String errMsg = "Problem de-serializing DistributorContact. " + e.getMessage();
			logger.error(errMsg);
			throw new IOException(errMsg);
		}
	}
	
	/*-----------------------------------------------------
	| toConfigStr() 
	------------------------*/
	
	/**
	 * Convert this object into a string suitable for storing
	 * as a property list value. The object can be reconstituted
	 * from this string via instanceOf(str).
	 * 
	 * @return String for storing as property list value.
	 */
	public String toConfigStr() {
		AsciiStringOutputStream outStream = null;
		try {
			outStream = new AsciiStringOutputStream();
		} catch (SecurityException e) {
		} catch (IOException e) {
		}
		try {
			writeObject(outStream);
		} catch (IOException e) {
			String errMsg = "Cannot serialize DistributorContact object for storage in property list:\n" +
			e.getMessage();
			logger.error(errMsg);
		}
		//String res = outStream.toString();
		return outStream.toString();
	}
     
	/*-----------------------------------------------------
	| restoreFromConfigurationStr() 
	------------------------*/
	
	/**
	 * Reconstitute a DistributorContact from a string that was
	 * created via toConfigStr();
	 * 
	 * @param configStr The string created by toConfigStr().
	 * @return A new DistributorContact with all the fields instantiated.
	 */
	public static DistributorContact instanceOf(String configStr) {
		DistributorContact me = new DistributorContact();
		ByteArrayInputStream inStream = new ByteArrayInputStream(configStr.getBytes());
		try {
			me.readObject(new ObjectInputStream(inStream));
		} catch (IOException e) {}
		catch (ClassNotFoundException e) {}
		return me;
	}

	/*-----------------------------------------------------
	| toString() 
	------------------------*/
	
	public String toString() {
		return "DistributorContact[" + crawlName + ":" + crawlType + "]";
	}
	
	/*-----------------------------------------------------
	| Getters 
	------------------------*/


	public String getDistributorMachineName() {
		return distributorMachineName;
	}

	public int getDistributorPort() {
		return distributorPort;
	}
	
	public String getDistributorPortAsStr() {
		return (new Integer(distributorPort)).toString();
	}
	
	public String getDistributorDate() {
		return distributorDate;
	}

	public int getNumPages() {
		return numPages;
	}

	public int getNumPagesWanted() {
		return numPagesWanted;
	}
	
	public String getCrawlName() {
		return crawlName;
	}

	public String getCrawlType() {
		return crawlType;
	}

	public String getSiteListPath() {
		return siteListPath;
	}

	
}
