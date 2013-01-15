package edu.stanford.pigir.warc;

/**
 * Container for a generic Warc Record 
 * 
 * (C) 2009 - Carnegie Mellon University
 * 
 * 1. Redistributions of this source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer. 
 * 2. The names "Lemur", "Indri", "University of Massachusetts",  
 *    "Carnegie Mellon", and "lemurproject" must not be used to 
 *    endorse or promote products derived from this software without
 *    prior written permission. To obtain permission, contact 
 *    license@lemurproject.org.
 *
 * 4. Products derived from this software may not be called "Lemur" or "Indri"
 *    nor may "Lemur" or "Indri" appear wbRecordReader their names without prior written
 *    permission of The Lemur Project. To obtain permission,
 *    contact license@lemurproject.org.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE LEMUR PROJECT AS PART OF THE CLUEWEB09
 * PROJECT AND OTHER CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF 
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN 
 * NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY 
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS 
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, 
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING 
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE. 
 * 
 * @author mhoy@cs.cmu.edu (Mark J. Hoy)
 * 
 * Jan 17, 2011; Andreas Paepcke: added inheritance from Text
 * Jan 19, 2011; Andreas Paepcke: modified to fit wbRecordReader Hadoop/Pig workflow. 
 *                                Replaced separate header API with a 
 *                                Map<String,String> implementation that
 *                                includes 'content' as one of its fields.
 * Jan 15, 2013; Andreas Paepcke: added ability to accept multiple WARC versions. 
 * 								  Search for WARC_VERSIONS, and add new ones as
 * 								  appropriate.  
 */

import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;

import edu.stanford.pigir.pigudf.LineAndChunkReader;


public class WarcRecord extends Text implements WarcRecordMap {

	// Class variables:
	
	public static final String CONTENT = "content";

	// Lookup table for properly capitalized ISO Warc header field
	// names. Used wbRecordReader toString();
	@SuppressWarnings("serial")
	private static final Map<String, String> ISO_WARC_HEADER_FIELD_NAMES = new HashMap<String, String>(){
		{
			put(WARC_TYPE, "WARC-Type");
			put(WARC_RECORD_ID, "WARC-Record-ID");
			put(WARC_DATE, "WARC-Date");
			put(CONTENT_LENGTH, "Content-Length");
			put(CONTENT_TYPE, "Content-Type");
			put(WARC_CONCURRENT_TO, "WARC-Concurrent-To");
			put(WARC_BLOCK_DIGEST, "WARC-Block-Digest");
			put(WARC_PAYLOAD_DIGEST, "WARC-Payload-Digest");
			put(WARC_IP_ADDRESS, "WARC-IP-Address");
			put(WARC_REFERS_TO, "WARC-Refers-To");
			put(WARC_TARGET_URI, "WARC-Target-URI");
			put(WARC_TRUNCATED, "WARC-Truncated");
			put(WARC_WARCINFO_ID, "WARC-Warcinfo-ID");
			put(WARC_FILENAME, "WARC-Filename");
			put(WARC_PROFILE, "WARC-Profile");
			put(WARC_IDENTIFIED_PAYLOAD_TYPE, "WARC-Identified-Payload-Type");
			put(WARC_SEGMENT_ORIGIN_ID, "WARC-Segment-Origin-ID");
			put(WARC_SEGMENT_NUMBER, "WARC-Segment-Number");
			put(WARC_SEGMENT_TOTAL_LENGTH, "WARC-Segment-Total-Length");
		}
	};

	// All lower-case WARC header field names:
	public static final String WARC_TYPE = "warc-type";
	public static final String WARC_RECORD_ID = "warc-record-id";
	public static final String WARC_DATE = "warc-date";
	public static final String CONTENT_LENGTH = "content-length";
	public static final String CONTENT_TYPE = "content-type";
	public static final String WARC_CONCURRENT_TO = "warc-concurrent-To";
	public static final String WARC_BLOCK_DIGEST = "warc-block-digest";
	public static final String WARC_PAYLOAD_DIGEST = "warc-payload-digest";
	public static final String WARC_IP_ADDRESS = "warc-ip-address";
	public static final String WARC_REFERS_TO = "warc-refers-to";
	public static final String WARC_TARGET_URI = "warc-target-uri";
	public static final String WARC_TRUNCATED = "warc-truncated";
	public static final String WARC_WARCINFO_ID = "warc-warcinfo-id";
	public static final String WARC_FILENAME = "warc-filename";
	public static final String WARC_PROFILE = "warc-profile";
	public static final String WARC_IDENTIFIED_PAYLOAD_TYPE = "warc-identified-payload-type";
	public static final String WARC_SEGMENT_ORIGIN_ID = "warc-segment-origin-id";
	public static final String WARC_SEGMENT_NUMBER = "warc-segment-number";
	public static final String WARC_SEGMENT_TOTAL_LENGTH = "warc-segment-total-length";
		
	private static final String[] mandatoryHeaderFields = {WARC_RECORD_ID,
														   CONTENT_LENGTH,
														   WARC_DATE,
														   WARC_TYPE
	};
	
	// Provide a constructor for each of the header datatypes:
	private static Constructor<String> strConstructor = null;
	private static Constructor<Integer> intConstructor = null;
	
	{
		try {
			strConstructor = String.class.getConstructor(String.class);
			intConstructor = Integer.class.getConstructor(String.class);
		} catch (SecurityException e1) {
			e1.printStackTrace();
		} catch (NoSuchMethodException e1) {
			e1.printStackTrace();
		}
	}

	@SuppressWarnings({ "rawtypes", "serial" })
	public HashMap<String,Constructor> mandatoryWarcHeaderFldTypes = new HashMap<String, Constructor>() {
		{
			put(WARC_RECORD_ID, strConstructor);
			put(CONTENT_LENGTH, intConstructor);
			put(WARC_DATE, strConstructor);
			put(WARC_TYPE, strConstructor);
		}
	};
	
	public static final boolean INCLUDE_CONTENT = true; 
	public static final boolean DONT_INCLUDE_CONTENT = false; 
	
	// Fast method for looking up whether a header key is mandatory or not:
	@SuppressWarnings("serial")
	private static final HashMap<String,Boolean> mandatoryHeaderFieldsLookup = new HashMap<String, Boolean>() {
		{
			for (String key : mandatoryHeaderFields) {
				put(key, true);
			}
		}
	};
	
	// Marker to look for when finding the next WARC record wbRecordReader a stream:
	public static String[] WARC_VERSIONS = {"WARC/0.18", "WARC/1.0"};
	//public static String WARC_VERSION_LINE = "WARC/0.18\n";
	private static String NEWLINE="\n";
	
	private static HashMap<String,String> tmpHeaderMap = new HashMap<String, String>();
	private static Long tmpGrandTotalBytesRead = 0L;
	private static HashSet<String> tmpOptionalHeaderKeys = new HashSet<String>();
	
	// Instance variables:
	private HashMap<String,String> headerMap = null;
	private Long grandTotalBytesRead;
	private byte[] warcContent=null;
	private HashSet<String> optionalHeaderKeysThisRecord;

	/**
	 * The actual heavy lifting of reading wbRecordReader the next WARC record. The
	 * readContent parameter is used to support cases when the original
	 * Pig query project out the content. We save time if we don't need
	 * that content.
	 * 
	 * @param warcLineReader a line reader
	 * @param readContent indicate whether the content of the record is needed, as opposed to just the WARC header info.
	 * @return the content bytes (w/ the headerBuffer populated)
	 * @throws java.io.IOException
	 */
	private static byte[] readNextRecord(LineAndChunkReader warcLineReader, boolean readContent) throws IOException {
		if (warcLineReader==null) { 
			return null;
		}

		Text txtBuf = new Text();
		byte[] retContent=null;
		
		tmpOptionalHeaderKeys.clear();
		tmpGrandTotalBytesRead = 0L;
		tmpHeaderMap.clear();
		// Find our WARC header
		boolean foundWARCHeader = scanToRecordStart(warcLineReader, txtBuf);
		txtBuf.clear();
		
		// No WARC header found?
		if (!foundWARCHeader) { return null; }

		// Read the header (up to the first empty line).
		// Make sure we get the (mandatory) content length 
		// is wbRecordReader the header, because we rely on it below. 
		// We do not check for the other mandatory header fields:
		int contentLength = pullHeaderFromStream(warcLineReader, 
												 txtBuf);
		txtBuf.clear();
		
		if (contentLength < 0) {
			return null;
		}

		if (readContent) {
			// Pull the bytes of the content from the stream:
			retContent=new byte[contentLength];
			Integer totalRead = pullContent(warcLineReader, retContent, contentLength);
			if (totalRead == null)
				throw new IOException("Could not read content from WARC record ID: " +
						tmpHeaderMap.get(WARC_RECORD_ID) + 
						" of supposed content length " +
						tmpHeaderMap.get(CONTENT_LENGTH) +
				". Reason is other than EOF.");

			if (totalRead < contentLength) {
				// Did we hit EOF wbRecordReader the middle of the WARC record's content?
				throw new IOException("Hit end of file while reading content of WARC record ID: " +
						tmpHeaderMap.get(WARC_RECORD_ID) + 
						" of supposed content length " +
						tmpHeaderMap.get(CONTENT_LENGTH) +
				".");
			}
			tmpGrandTotalBytesRead += totalRead;
			return retContent;
		} else {
			return new byte[0];
		}
	}

	/**
	 * @param warcLineReader
	 * @param retContent
	 * @param contentLength
	 * @return
	 * @throws IOException
	 */
	private static Integer pullContent(LineAndChunkReader warcLineReader,
			byte[] retContent, int contentLength) throws IOException {
		int totalWant=contentLength;
		int totalRead=0;
		while (totalRead < contentLength) {
			try {
				int numRead=warcLineReader.read(retContent, totalRead, totalWant);
				if (numRead < 0) {
					return null;
				} else {
					totalRead += numRead;
					totalWant = contentLength-totalRead;
				} // end if (numRead < 0) / else
			} catch (EOFException eofEx) {
				// resize to what we have
				if (totalRead > 0) {
					return totalRead;
				} else {
					return null;
				}
			} // end try/catch (EOFException)
		} // end while (totalRead < contentLength)
		return totalRead;
	}

	/**
	 * @param warcLineReader
	 * @param txtBuf
	 * @param inHeader
	 * @return
	 * @throws IOException
	 */
	private static int pullHeaderFromStream(LineAndChunkReader warcLineReader,
			Text txtBuf) throws IOException {
		boolean inHeader = true;
		String line;
		int bytesRead;
		int contentLength=-1;
		String headerAttrName;
		String headerAttrValue;
		txtBuf.clear();
		while (inHeader && ((bytesRead = warcLineReader.readLine(txtBuf))!=0)) {
			line = txtBuf.toString();
			tmpGrandTotalBytesRead += bytesRead;
			if (line.trim().length()==0) {
				inHeader=false;
			} else {
				String[] thisHeaderPieceParts=line.split(":", 2);
				if (thisHeaderPieceParts.length==2) {
					headerAttrName  = (thisHeaderPieceParts[0]).trim().toLowerCase();
					headerAttrValue =  thisHeaderPieceParts[1].trim();
					tmpHeaderMap.put(headerAttrName, headerAttrValue);
					
					// Accumulate a list of optional header keys:
					if (mandatoryHeaderFieldsLookup.get(headerAttrName) == null)
						tmpOptionalHeaderKeys.add(headerAttrName);
					
					if (headerAttrName.startsWith(CONTENT_LENGTH)) {
						try {
							contentLength=Integer.parseInt(headerAttrValue.trim());
						} catch (NumberFormatException nfEx) {
							contentLength=-1;
						}
					}
				}
			}
			txtBuf.clear();
		}
		return contentLength;
	}

	/**
	 * @param warcLineReader
	 * @param txtBuf
	 * @return success true/false
	 * @throws IOException
	 */
	private static boolean scanToRecordStart(LineAndChunkReader warcLineReader,
											 Text txtBuf) throws IOException {
		String line;
		boolean foundMark = false;
		int bytesRead;
		while ((!foundMark) && ((bytesRead = warcLineReader.readLine(txtBuf))!=0)) {
			line = txtBuf.toString();
			tmpGrandTotalBytesRead += bytesRead;
			int i;
			for (i=0; i < WARC_VERSIONS.length; i++) {
				if (line.startsWith(WARC_VERSIONS[i])) {
					foundMark=true;
				}
			}
			txtBuf.clear();
		}
		return foundMark;
	}

	/**
	 * Reads wbRecordReader a WARC record from a data input stream
	 * @param Warc line reader for the stream.
	 * @return a WARC record (or null if eof)
	 * @throws java.io.IOException
	 */
	@SuppressWarnings("unchecked")
	public static WarcRecord readNextWarcRecord(LineAndChunkReader warcInLineReader, boolean readContent) throws IOException {
		
		byte[] recordContent=readNextRecord(warcInLineReader, readContent);
		if (recordContent==null) { 
			return null; 
		}

		WarcRecord retRecord=new WarcRecord();
		retRecord.headerMap = (HashMap<String, String>) tmpHeaderMap.clone();
		retRecord.grandTotalBytesRead = tmpGrandTotalBytesRead;
		retRecord.optionalHeaderKeysThisRecord = (HashSet<String>) tmpOptionalHeaderKeys.clone();
		retRecord.setRecordContent(recordContent);
		
		return retRecord;
	}

	/**
	 * Default Constructor
	 */
	public WarcRecord() {
	}

	/**
	 * Retrieves the total record length (header and content)
	 * @return total record length
	 */
	public Long getTotalRecordLength() {
		return grandTotalBytesRead;
	}


	protected void setRecordContent(byte[] content) {
		warcContent = content;
	}
	/**
	 * Retrieves the bytes content as a UTF-8 string
	 * @return
	 */
	public String getContentUTF8() {
		String retString=null;
		try {
			retString = new String(warcContent, "UTF-8");
		} catch (UnsupportedEncodingException ex) {
			retString=new String(warcContent);
		}
		return retString;
	}

	@Override
	public String toString() {
		return toString(DONT_INCLUDE_CONTENT);
	}
	
	public String toString(boolean shouldIncludeContent) {
		StringBuffer retBuffer=new StringBuffer();
		String headerVal;
		for (String headerFldNm : headerMap.keySet()) {
			retBuffer.append(ISO_WARC_HEADER_FIELD_NAMES.get(headerFldNm) + ":" + 
							 ((headerVal = headerMap.get(headerFldNm)) == null ? "" : headerVal) + "\n");
		}
		if (shouldIncludeContent) {
			retBuffer.append(NEWLINE);
			retBuffer.append(getContentUTF8());
		}
		else
			retBuffer.append("[Record content suppressed. Use toString(INCLUDE_CONTENT) to see the content string.\n");
		return retBuffer.toString();
	}
	
	//  -----------------------------------  MAP<String,String> Methods -----------------------

	public int size() {
		// Plus 1 is for the pseudo 'content' byte array
		// that's not really part of the hash:
		return headerMap.size() + 1;
	}

	public boolean isEmpty() {
		return headerMap.isEmpty() && (warcContent.length == 0); 
	}

	public boolean containsKey(Object key) {
		String lowerCaseKey = ((String) key).toLowerCase();
		return (headerMap.containsKey(lowerCaseKey) || lowerCaseKey.equals(CONTENT));
	}

	public boolean containsValue(Object value) {
		if (headerMap.containsValue(value))
			return true;
		String content = getContentUTF8();
		return content.contains((String) value);
	}

	public String get(Object key) {
		if (((String) key).equalsIgnoreCase(CONTENT)) {
			return getContentUTF8();
		}
		return headerMap.get(((String)key).toLowerCase());
	}

	public String put(String key, String value) {
		String prevValue;
		String lowerCaseKey = key.toLowerCase();
		if (lowerCaseKey.equals(CONTENT)) {
			prevValue = getContentUTF8();
			warcContent = value.getBytes();
			return prevValue;
		}
		prevValue = headerMap.get(lowerCaseKey);
		headerMap.put(lowerCaseKey, value);
		return prevValue;
	}

	public String remove(Object key) {
		String prevValue;
		String lowerCaseKey = ((String)key).toLowerCase();
		if (lowerCaseKey.equalsIgnoreCase(CONTENT)) {
			prevValue = getContentUTF8();
			warcContent = new byte[0];
			return prevValue;
		}
		return headerMap.remove(lowerCaseKey);
	}

	public void putAll(Map<? extends String, ? extends String> m) {
		for (String key : m.keySet()) {
			put(key, m.get(key));
		}
	}

	public Set<String> keySet() {
		Set<String> res = headerMap.keySet();
		res.add(CONTENT);
		return res;
	}
	
	public Set<String> keySetHeader() {
		return headerMap.keySet();
	}
	
	public String[] mandatoryKeysHeader() {
		return mandatoryHeaderFields;
	}

	public Set<String> optionalKeysHeader() {
		return optionalHeaderKeysThisRecord;
	}
	
	public String[] mandatoryValuesHeader() {
		String[] res = new String[mandatoryHeaderFields.length];
		for (int i=0; i<mandatoryHeaderFields.length; i++) {
			res[i] = get(mandatoryHeaderFields[i]);
		}
		return res;
	}
	
	public Collection<String> values() {
		Collection<String> res = headerMap.values();
		res.add(getContentUTF8());
		return res;
	}
	
	public Collection<String> valuesHeader() {
		return headerMap.values();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Set entrySet() {
		return entrySet(true);
	}
	
	public Set<Entry<String,String>> entrySet(boolean readContent) {
		//Set<Entry> res = new HashSet<Entry>();
		HashSet<Entry<String,String>> res = new HashSet<Entry<String,String>>();
		for (Map.Entry<String, String> headerMapEntry : headerMap.entrySet()){
			res.add(new Entry<String,String>(headerMapEntry.getKey(), headerMapEntry.getValue()));
		}
		if (readContent) {
			res.add(new Entry<String,String>(CONTENT, getContentUTF8()));
		}
			
		return res;
	}
	
	private class Entry<K,V> implements Map.Entry<K,V> {

		K key;
		V value;
		
		public Entry(K theKey, V theValue) {
			key = theKey;
			value = theValue;
		}
		
		public K getKey() {
			return key;
		}

		public V getValue() {
			return value;
		}

		public V setValue(V theValue) {
			V oldVal = value;
			value = theValue;
			return oldVal;
		}
		
		@SuppressWarnings("unchecked")
		public boolean equals (Object obj) {
			if (!obj.getClass().equals(this.getClass()))
				return false;
			return (((Entry<K,V>)obj).getKey().equals(key) && ((Entry<K,V>)obj).getValue().equals(value));	
		}
		
		public int hashCode() {
			return ((key==null   ? 0 : key.hashCode()) ^  (value == null ? 0 : value.hashCode()));
		}
		
		public String toString() {
			return new String(key + "=" + value);
		}
	}
}

