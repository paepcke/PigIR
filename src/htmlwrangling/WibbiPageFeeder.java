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
public class WibbiPageFeeder implements Iterator<WibbiPageFeeder.WebBasePage> {
	
	public static final boolean FLUSH_REST_OF_PAGE = true; 
	public static final boolean PRESERVE_PAGE_POSITION = false; 
	
	private static final String PAGE_SEPARATOR = "==P=>>>>=i===<<<<=T===>=A===<=!Junghoo!==>";
	// Buffer read-ahead limit when page size is unknown. This 
	// condition is true for the very start of the stream:
	private static final int READ_AHEAD_DEFAULT_LIMIT = 4096;
	
	BufferedReader webBaseFileSource = null;
	int currDocLen = -1;
	WebBasePage currPage = null;

	public WibbiPageFeeder(BufferedReader theReader) {
		webBaseFileSource = theReader;
	}
	
	public WibbiPageFeeder(Reader theReader) {
		webBaseFileSource = new BufferedReader(theReader);
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
			webBaseFileSource.mark(currDocLen < 0 ? READ_AHEAD_DEFAULT_LIMIT : currDocLen);
			boolean pageDelimiterAhead = pastNextPageSeparator();
			if (pageCursorPreservation == PRESERVE_PAGE_POSITION)
				webBaseFileSource.reset();
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

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 * Return next Web page, or null if any problems arise in the parsing.
	 * A null return does not mean that the next page won't work either.
	 * Only hasNext() answers that question. Null returns happen, for instance
	 * if no Content-Length field is found in the HTTP header.
	 */
	@Override
	public WibbiPageFeeder.WebBasePage next() {
		String line;
		String url = null;
		int colonPos = -1;
		try {
			// First line should be the first line of the WebBase header:
			// URL: http://www....:
			line = webBaseFileSource.readLine();
			if (!line.startsWith("URL: ")) {
				if (!pastNextPageSeparator())
					// return an empty WebBasePage object:
					return new WebBasePage();
				line = webBaseFileSource.readLine();
				if (!line.startsWith("URL: "))
					// return an empty WebBasePage object:
					return new WebBasePage();
			}
			// We know we are at the start of the WebBase header. Grab info
			// from WebBase header, then from the HTTP header:
			if ((colonPos = line.indexOf(':')) == -1) {
				// Ill-formed page:
				// return an empty WebBasePage object:
				return new WebBasePage();
			}
			url = line.substring(colonPos + 1).trim();
			
			// Advance cursor past the WebBase header to start of HTTP header:
			for (int i=0;i<4;i++)
				webBaseFileSource.readLine();
			// Grab HTTP header:
			currPage = new WebBasePage(url, webBaseFileSource);
			currDocLen = currPage.contentLength;
			if (currDocLen == -1)
				return currPage;
			
			// Cursor points past the HTTP header terminating 
			// empty line to the first byte of the page. Read the
			// page:
			char[] page = new char[currDocLen];
			webBaseFileSource.read(page, 0, currDocLen);
			currPage.put(Constants.CONTENT_KEY, new String(page));
			return currPage;
		} catch (IOException e) {
			// return an empty WebBasePage object:
			return new WebBasePage();
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
			while ((line = webBaseFileSource.readLine()) != null) {
				if (line.equals(PAGE_SEPARATOR))
					return true;
			}
		} catch (IOException e) {}
		return false;
	}

	// ----------------------------------  Class WebBasePageInfo --------------------------
	
	
	/**
	 * Class to hold some of the WebBase header, and all of the HTTP header
	 * of one Web page in the page source. Instances implement the Map interface.
	 * Instances of the class behave like a Map. The HTTP header fields can be
	 * retrieved via <anInstance>.get("<HTTP-header-field-lower-cased>");
	 * The page content is held under the key htmlwrangling.Constants.CONTENT_KEY
	 * 
	 * @author paepcke
	 *
	 */
	public class WebBasePage implements Map<String,String>{
		
		String url = "";
		int contentLength = -1;
		HashMap<String,String> httpHeader = new HashMap<String,String>();
		
		public WebBasePage(String theUrl, BufferedReader source) {
			url = theUrl;
			initHTTPHeaderInfo(source);
		} 
		
		public WebBasePage() {
			// this constructor is used to return an empty 
			// result from the next() method above.
		}
		
		public void initHTTPHeaderInfo(BufferedReader source) {
			String line;
			int colonPos = -1;
			try {
				while (true) {
					line = source.readLine();
					if (line.isEmpty()) {
						// Header done:
						try {
							contentLength = Integer.parseInt(httpHeader.get("content-length"));
						} catch (NumberFormatException e) {
							contentLength = -1;
						}
						return;
					}
					// Separate filed name from field value:
					if ((colonPos = line.indexOf(':')) == -1)
						continue;
					// Don't get derailed by a weirdly formed header field (e.g. no value):
					try {
						// Lower-case all HTTP header field names:
						httpHeader.put(line.substring(0, colonPos).trim().toLowerCase(), 
									   line.substring(colonPos + 1).trim());
					} catch (Exception e) {
						continue;
					}
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
		
		public String toString() {
			return "<WBPage len=" + contentLength + "; " +
					"url=" + (url.isEmpty() ? "NA" : url) + 
					">";
		}
	}
	
	// ------------------------------------  Testing -----------------------------------
	
	public static void main(String[] args) {
	
		String singlePage = "==P=>>>>=i===<<<<=T===>=A===<=!Junghoo!==>\n" + 
		"URL: http://www.state.gov/robots.txt\n" + 
		"Date: \n" + 
		"Position: 0\n" + 
		"DocId: 0\n" + 
		"\n" + 
		"HTTP/1.0 200 OK\n" + 
		"Server: Apache\n" + 
		"ETag: 'ed2c2598845076d83690ba59b058e231:1292500314'\n" + 
		"Last-Modified: Thu, 16 Dec 2010 11:51:54 GMT\n" + 
		"Accept-Ranges: bytes\n" + 
		"Content-Length: 159\n" + 
		"Content-Type: text/plain\n" + 
		"Expires: Thu, 16 Dec 2010 22:21:06 GMT\n" + 
		"Cache-Control: max-age=0, no-cache, no-store\n" + 
		"Pragma: no-cache\n" + 
		"Date: Thu, 16 Dec 2010 22:21:06 GMT\n" + 
		"Connection: close\n" + 
		"\n" + 
		"# tell scanning search robots not to index the older archive pages\n" + 
		"#\n" + 
		"User-agent: *\n" + 
		"Disallow: /www/\n" + 
		"Disallow: /waterfall/\n" + 
		"Disallow: /menu/\n" + 
		"Disallow: /navitest/\n" + 
		"\n" + 
		"==P=>>>>=i===<<<<=T===>=A===<=!Junghoo!==>\n" + 
		"0";

		StringReader pageReader = new StringReader(singlePage);
		WibbiPageFeeder feeder = new WibbiPageFeeder(pageReader);
		while (feeder.hasNext(WibbiPageFeeder.FLUSH_REST_OF_PAGE)) {
			WibbiPageFeeder.WebBasePage page = feeder.next();
			System.out.println(page.get(Constants.CONTENT_KEY));
		}
	}
}
