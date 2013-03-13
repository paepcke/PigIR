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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;


public class PigWarcRecord extends Text implements WarcRecordMap {

	// Class variables:
	
	public static final String CONTENT = "content";
	public static final String WARC_VERSION = "warc-version";

	// Lookup table for properly capitalized ISO Warc header field
	// names. Used wbRecordReader toString();
	@SuppressWarnings("serial")
	public static final Map<String, String> ISO_WARC_HEADER_FIELD_NAMES = new HashMap<String, String>(){
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
		
	public static final String[] mandatoryHeaderFields = {WARC_RECORD_ID,
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
	
	// Marker to look for when finding the next WARC record wbRecordReader a stream:
	public static String[] WARC_VERSIONS = {"WARC/0.18", "WARC/1.0"};
	//public static String WARC_VERSION_LINE = "WARC/0.18\n";
	private static String NEWLINE="\n";
	
	// Instance variables:
	protected HashMap<String,String> headerMap = null;
	protected byte[] warcContent=new byte[0];
	protected HashSet<String> optionalHeaderKeysThisRecord;
	protected String warcVersion = null;


	public PigWarcRecord() {
		
	}
	
	/**
	 * Make a WARC record from a Pig tuple
	 * @throws IOException 
	 */
	public PigWarcRecord(Tuple warcTuple) throws IOException {
		if (warcTuple.size() < mandatoryWarcHeaderFldTypes.size()) {
			throw new IOException("WARC tuple '" + warcTuple.toString() + "' has fewer than required fields.");
		}
		headerMap = new LinkedHashMap<String, String>();
		try {			
			this.put(WARC_RECORD_ID, (String) warcTuple.get(0));
			this.put(CONTENT_LENGTH, (String) warcTuple.get(1));
			this.put(WARC_DATE, (String) warcTuple.get(2));
			this.put(WARC_TYPE, (String) warcTuple.get(3));
			
			// The optional header fields are stored in a bag:
			if (warcTuple.size() > mandatoryWarcHeaderFldTypes.size()) {
				// Yep, got optional header bag in next tuple field:
				DataBag optionalHeaderFieldBag = (DataBag) warcTuple.get(4);
				Iterator<Tuple>optHeaderIt = optionalHeaderFieldBag.iterator();
				String headFldName = null;
				String headFldVal  = null;
				while (optHeaderIt.hasNext()) {
					Tuple headFldNameVal = optHeaderIt.next();
					
					try {
						headFldName = (String)headFldNameVal.get(0);
						headFldVal  = (String)headFldNameVal.get(1);
					} catch (Exception e) {
						throw new IOException("Error extracting optional WARC header fields from WARC tuple '" + warcTuple.toString() + "':" +
											  e.getMessage());
					}
					this.put(headFldName, headFldVal);
				}
			}
			// If tuple also has content field, get it into the new WarcRecord as well:
			if (warcTuple.size() > mandatoryWarcHeaderFldTypes.size() + 1) {
				try {
					String content = (String)warcTuple.get(5);
					this.put(CONTENT, content);
				} catch (Exception e) {
					throw new IOException("Error during reading of content field from WARC tuple '" + warcTuple.toString() + "':" +
								          e.getMessage());				
				}
			}
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new IOException("Error while processing WARC tuple '" + warcTuple.toString() + "':" +
								          e.getMessage());		
		}
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
	
	/**
	 * Returns content as byte array.
	 * @return
	 */
	public byte[] getContentRaw() {
		return warcContent;
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
		return (headerMap.containsKey(lowerCaseKey) || lowerCaseKey.equals(CONTENT) || lowerCaseKey.equals(WARC_VERSION));
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
		if (((String) key).equalsIgnoreCase(WARC_VERSION)) {
			return warcVersion;
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
		if (lowerCaseKey.equals(WARC_VERSION)) {
			prevValue = warcVersion;
			warcVersion = value;
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
		if (lowerCaseKey.equalsIgnoreCase(WARC_VERSION)) {
			prevValue = warcVersion;
			warcVersion = null;
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
		res.add(WARC_VERSION);
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
		res.add(new Entry<String,String>(WARC_VERSION, warcVersion));
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

