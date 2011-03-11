package htmlwrangling;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Given a Reader that is assumed to read from a Junghoo separated file,
 * provides an iterator to get the pages.
 * 
 * @author paepcke
 *
 */
public class WibbiPageFeeder implements Iterator<String> {
	
	public static boolean FLUSH_REST_OF_PAGE = true; 
	public static boolean PRESERVE_PAGE_POSITION = false; 
	
	private static final String JUNGOO_SEPARATOR = "==P=>>>>=i===<<<<=T===>=A===<=!Junghoo!==>";
	// Buffer read-ahead limit when page size is unknown. This 
	// condition is true for the very start of the stream:
	private static final int READ_AHEAD_DEFAULT_LIMIT = 4096;
	
	BufferedReader junghooSource = null;
	int currDocLen = -1;
	WebBasePageInfo currPageInfo = null;

	public WibbiPageFeeder(BufferedReader theReader) {
		junghooSource = theReader;
	}
	
	public WibbiPageFeeder(Reader theReader) {
		junghooSource = new BufferedReader(theReader);
	}
	
	public WibbiPageFeeder(File theFile) throws FileNotFoundException {
		this(new FileReader(theFile));
	}
	
	public WibbiPageFeeder(String theFileName) throws FileNotFoundException {
		this(new FileReader(theFileName));
	}
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 * Efficiency boosting variant of the standard iterator's hasNext() method. 
	 * A boolean parameter allows caller to declare whether the method is
	 * to scan forward in the input to just past the next page separator,
	 * flushing the remainder of the current page, or whether to save the
	 * cursor, and roll the input back to the cursor's position when called. 
	 * We check whether we find a page separator in the input stream.  
	 */

	public boolean hasNext(boolean pageCursorPreservation) {
		// BufferedReader supports marking, so mark
		// the current stream position, and check for page
		// separator in the stream ahead.
		try {
			junghooSource.mark(currDocLen < 0 ? READ_AHEAD_DEFAULT_LIMIT : currDocLen);
			boolean pageDelimiterAhead = pastNextPageSeparator();
			if (pageCursorPreservation == PRESERVE_PAGE_POSITION)
				junghooSource.reset();
			return pageDelimiterAhead;
		} catch (IOException e) {
			// Should not happen with a BufferedReader
			e.printStackTrace();
		}
		return false;
	}
	
	@Override
	public boolean hasNext() {
		return hasNext(PRESERVE_PAGE_POSITION);
	}

	@Override
	public String next() {
		String line;
		String[] urlKeyVal;
		try {
			// First line should be the first line of the WebBase header:
			// URL: http://www....:
			line = junghooSource.readLine();
			if (!line.startsWith("URL: ")) {
				if (!pastNextPageSeparator())
					return null;
				line = junghooSource.readLine();
				if (!line.startsWith("URL: "))
					return null;
			}
			// We know we are at the start of the WebBase header. Grab info
			// from WebBase header, then from the HTTP header:
			urlKeyVal = line.split(":");
			if (urlKeyVal.length != 2)
				return null;
			// Advance cursor to start of HTTP header:
			for (int i=0;i<4;i++)
				junghooSource.readLine();
			// Grap HTTP header:
			currPageInfo = new WebBasePageInfo(urlKeyVal[1]);
			try {
				currDocLen = Integer.parseInt(currPageInfo.get("Content-Length"));
			} catch (NumberFormatException e) {
				throw new RuntimeException("Content length in HTTP header is not an integer: " + currPageInfo);
			}
			
			// Cursor points past the HTTP header terminating 
			// empty line to the first byte of the page. Read the
			// page:
			char[] page = new char[currDocLen];
			junghooSource.read(page, 0, currDocLen);
			return(new String(page));
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public void remove() {
		// Removal of a page makes no sense here.
		throw new RuntimeException("Cannot remove pages from WebBase file.");
	}
	
	/**
	 * Advance read cursor to past next page separator, if one is found.
	 * @return True if a page separator was found. Else returns false.
	 */
	private boolean pastNextPageSeparator() {
		try {
			String line;
			while ((line = junghooSource.readLine()) != null) {
				line = junghooSource.readLine();
				if (line.equals(JUNGOO_SEPARATOR))
					return true;
			}
		} catch (IOException e) {}
		return false;
	}

	// ----------------------------------  Class WebBasePageInfo --------------------------
	
	
	/**
	 * Class to hold some of the WebBase header, and all of the HTTP header
	 * of one Web page in the page source. Instances implement the Map interface.
	 * 
	 * @author paepcke
	 *
	 */
	class WebBasePageInfo implements Map<String,String>{
		
		private final byte CR = '\r';
		
		String url = "";
		int contentLength = 0;
		HashMap<String,String> httpHeader = new HashMap<String,String>();
		
		public WebBasePageInfo(String theUrl) {
			url = theUrl;
		} 
		
		public void initHTTPHeaderInfo(BufferedReader source) {
			String line;
			String[] httpHeaderFields;
			byte[] lineBytes;
			try {
				while (true) {
					line = source.readLine();
					lineBytes = line.getBytes();
					if (lineBytes[0] == CR)
						// Header done:
						return;
					httpHeaderFields = line.split(":");
					if (httpHeaderFields.length != 2)
						continue;
					httpHeader.put(httpHeaderFields[0].trim(), httpHeaderFields[1].trim());
				}
			} catch (IOException e) {
				return;
			}
		}

		@Override
		public void clear() {
			url = "";
			contentLength = 0;
			httpHeader.clear();
		}

		@Override
		public boolean containsKey(Object theKey) {
			String key = ((String) theKey).toLowerCase();
			return ((key.equals("url")) ||
					httpHeader.containsKey(theKey));
		}

		@Override
		public boolean containsValue(Object theValue) {
			return (httpHeader.containsValue(theValue) ||
					url.equals(theValue));
		}

		@Override
		public Set<java.util.Map.Entry<String, String>> entrySet() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String get(Object theKey) {
			String key = ((String) theKey).toLowerCase();
			if (key.equals("url"))
				return url;
			else
				return httpHeader.get(theKey);
		}

		@Override
		public boolean isEmpty() {
			return(url.isEmpty() && httpHeader.isEmpty());
		}

		@Override
		public Set<String> keySet() {
			Set<String> keys = httpHeader.keySet();
			keys.add("url");
			return keys;
		}

		@Override
		public String put(String theKey, String theValue) {
			String prev = get(theKey);
			String key = ((String) theKey).toLowerCase();
			if (key.equals("url"))
				url = theValue;
			else
				httpHeader.put(theKey, theValue);
			return prev;
		}

		@Override
		public void putAll(Map<? extends String, ? extends String> existingMap) {
			for (String key : existingMap.keySet()) {
				put(key, existingMap.get(key));
			}
		}

		@Override
		public String remove(Object theKey) {
			String prev = get(theKey);
			String key = ((String) theKey).toLowerCase();
			if (key.equals("url"))
				url = "";
			else
				httpHeader.remove(theKey);
			return prev;
		}

		@Override
		public int size() {
			// Size is httpHeader HashMap size plus url:
			return httpHeader.size() + 1;
		}

		@Override
		public Collection<String> values() {
			Collection<String> vals = httpHeader.values();
			vals.add(get("url"));
			return vals;
		}
	}
	
	// ------------------------------------  Testing -----------------------------------
	
	public static void main(String[] args) {
	
		String singlePage = "==P=>>>>=i===<<<<=T===>=A===<=!Junghoo!==>" + 
		"URL: http://www.state.gov/robots.txt" + 
		"Date: " + 
		"Position: 0" + 
		"DocId: 0" + 
		"" + 
		"HTTP/1.0 200 OK" + 
		"Server: Apache" + 
		"ETag: 'ed2c2598845076d83690ba59b058e231:1292500314'" + 
		"Last-Modified: Thu, 16 Dec 2010 11:51:54 GMT" + 
		"Accept-Ranges: bytes" + 
		"Content-Length: 159" + 
		"Content-Type: text/plain" + 
		"Expires: Thu, 16 Dec 2010 22:21:06 GMT" + 
		"Cache-Control: max-age=0, no-cache, no-store" + 
		"Pragma: no-cache" + 
		"Date: Thu, 16 Dec 2010 22:21:06 GMT" + 
		"Connection: close" + 
		"" + 
		"# tell scanning search robots not to index the older archive pages" + 
		"#" + 
		"User-agent: *" + 
		"Disallow: /www/" + 
		"Disallow: /waterfall/" + 
		"Disallow: /menu/" + 
		"Disallow: /navitest/" + 
		"" + 
		"==P=>>>>=i===<<<<=T===>=A===<=!Junghoo!==>" + 
		"0";

		StringReader pageReader = new StringReader(singlePage);
		WibbiPageFeeder feeder = new WibbiPageFeeder(pageReader);
		while (feeder.hasNext(WibbiPageFeeder.FLUSH_REST_OF_PAGE)) {
			String page = feeder.next();
			System.out.println(page);
		}
			
	}
}
