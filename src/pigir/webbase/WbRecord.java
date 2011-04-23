package pigir.webbase;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * WbRecord implements the Java Map interface. So all fields
 * are available via 'get("key")'. The WebBase header keys are
 * defined in constants below. The HTTP header keys are as per
 * the HTTP specification. E.g. 'Content-length', 'content-length',
 * 'Content-type' etc. Case is ignored.
 *   
 * @author paepcke
 *
 */
public abstract class WbRecord extends Text implements WbRecordMap<String, Object> {
	
	protected Logger logger;
	
	public static final boolean INCLUDE_CONTENT = true; 
	public static final boolean DONT_INCLUDE_CONTENT = false; 
	
	// All lower-case WebBase header field names:
	public static final String WEBBASE_URL = "webbase-url";
	public static final String WEBBASE_DATE = "webbase-date";
	public static final String WEBBASE_SIZE = "webbase-pageSize";
	public static final String WEBBASE_POSITION = "webbase-position";
	public static final String WEBBASE_DOCID = "webbase-docid";
	public static final String CONTENT = "content";
	public static final String HTTP_HEADER_MAP = "http-header";
	
	
	protected Metadata md;

	private HashMap<String,String> httpHeader = new HashMap<String,String>();
	protected byte[] wbContent=null;
	
	private static final String[] mandatoryHeaderFields = {WEBBASE_URL,
														   WEBBASE_DATE,
														   WEBBASE_SIZE,
														   WEBBASE_POSITION,
														   WEBBASE_DOCID
	};
		
	// Provide a constructor for each of the header datatypes:
	private static Constructor<String> strConstructor = null;
	private static Constructor<Integer> intConstructor = null;
	private static Constructor<Long> longConstructor = null;
	
	{
		try {
			strConstructor = String.class.getConstructor(String.class);
			intConstructor = Integer.class.getConstructor(String.class);
			longConstructor = Long.class.getConstructor(String.class);
		} catch (SecurityException e1) {
			e1.printStackTrace();
		} catch (NoSuchMethodException e1) {
			e1.printStackTrace();
		}
	}

	@SuppressWarnings({ "rawtypes", "serial" })
	public HashMap<String,Constructor> mandatoryWbHeaderFldTypes = new HashMap<String, Constructor>() {
		{
			put(WEBBASE_URL, strConstructor);
			put(WEBBASE_DATE, strConstructor);
			put(WbRecord.WEBBASE_SIZE, intConstructor);
			put(WEBBASE_POSITION, longConstructor);
			put(WEBBASE_DOCID, intConstructor);
		}
	};
	
	public abstract WebContentType getContentType();

	/*-----------------------------------------------------
	| Constructors 
	------------------------*/
	
	public WbRecord(Metadata md, String httpHeader, byte[] content){
		this(md, WbRecordFactory.getHTTPHeader(httpHeader), content);
	}
	
	public WbRecord(Metadata md, Vector<String> httpHeaderVec, byte[] content) {
		this.md = md;
		this.wbContent = content;
		this.logger = WbRecordReader.getLogger();
		
		if (httpHeaderVec.size() < 1)
			return;
		
		if (httpHeaderVec.get(0).startsWith("HTTP")) {
			this.httpHeader.put("Server-response", httpHeaderVec.get(0));
		}
		String headerLine;
		int colonIndx;
		for (int i=1; i<httpHeaderVec.size(); i++) {
			headerLine = httpHeaderVec.get(i);
			// Can't conveniently use split(":") b/c date/time has colons.
			colonIndx = headerLine.indexOf(':');
			if (colonIndx < 0)
				continue;
			this.httpHeader.put(headerLine.substring(0, colonIndx).trim(), 
								headerLine.substring(colonIndx + 1).trim());
		}
	}
	
	/*-----------------------------------------------------
	| getContent()
	------------------------*/
	
	public abstract Object getContent();
	
	/*-----------------------------------------------------
	| setContent() 
	------------------------*/
	
	protected void setContent(byte[] c) {
		this.wbContent = c;
	}
	
	/*-----------------------------------------------------
	| getMetadata() 
	------------------------*/

	public Metadata getMetadata() {
		return md;
	}
	
	/*-----------------------------------------------------
	| setMetadata()
	------------------------*/
	protected void setMetadata(Metadata d) {
		this.md = d;
	}
	
	/*-----------------------------------------------------
	| getHTTPHeaderAsString()
	------------------------*/
	public String getHTTPHeaderAsString() {
		String retString = "";
		for (String key : httpHeader.keySet()) {
			retString += key + "=" + httpHeader.get(key) + "\r\n";
		}
		return retString;
	}
	
	/*-----------------------------------------------------
	| toString()
	------------------------*/
	public String toString () {
		return toString(DONT_INCLUDE_CONTENT);
	}
	
	public String toString(Boolean includeContent) {
		if (includeContent)
			return md.toString() + getHTTPHeaderAsString() + (new String(this.wbContent));
		else
			return md.toString() + getHTTPHeaderAsString();
	}
	
	/*-----------------------------------------------------
	| getContentUTF8() 
	------------------------*/
	/**
	 * Retrieves the bytes content as a UTF-8 string
	 * @return
	 */
	public String getContentUTF8() {
		String retString=null;
		try {
			retString = new String(wbContent, "UTF-8");
		} catch (UnsupportedEncodingException ex) {
			retString=new String(wbContent);
		}
		return retString;
	}
	
	/*-----------------------------------------------------
	| mandatoryKeysHeader()
	------------------------*/
	/* (non-Javadoc)
	 * @see pigir.webbase.WbRecordMap#mandatoryKeysHeader()
	 * Keys that are present in every WebBase page header.
	 */
	public String[] mandatoryKeysHeader() {
		return mandatoryHeaderFields;
	}
	
	/*-----------------------------------------------------
	| mandatoryValuesHeader()
	------------------------*/
	/* (non-Javadoc)
	 * @see pigir.webbase.WbRecordMap#mandatoryValuesHeader()
	 * Values that are present in every WebBase page header.
	 */
	public String[] mandatoryValuesHeader() {
		String[] res = new String[mandatoryHeaderFields.length];
		for (int i=0; i<mandatoryHeaderFields.length; i++) {
			res[i] = get(mandatoryHeaderFields[i]);
		}
		return res;
	}
	
	/*-----------------------------------------------------
	| isDefaultRecord
	------------------------*/
	
	/**
	 * Subclasses whose contents are strings can leave this method stand. But subclasses
	 * like those inheriting from WbBinaryRecord must override and return true;
	 *  
	 * @return true if this WbRecord is of subclass WbDefaultRecord. Else return false;
	 */
	public boolean isBinaryRecord() {
		return false;
	}
	
	//  -----------------------------------  MAP<String,String> Methods -----------------------

	@Override
	public int size() {
		// Plus 1 is for the pseudo 'content' byte array
		// that's not really part of the hash:
		return md.size() + 1;
	}

	@Override
	public boolean isEmpty() {
		return (md == null) && (wbContent.length == 0); 
	}

	public boolean isContentEmpty() {
		return (wbContent.length == 0);
	}
	
	/* (non-Javadoc)
	 * @see java.util.Map#containsKey(java.lang.Object)
	 * Looks through keys in the metadata, like docID, numPages, etc.,
	 * as well as through the HTTP header keys that are contained
	 * in the httpHeader HashMap, and the keys CONTENT and HTTP_HEADER_MAP.
	 */
	@Override
	public boolean containsKey(Object key) {
		String lowerCaseKey = ((String) key).toLowerCase();
		return (md.containsKey(lowerCaseKey) || 
				lowerCaseKey.equals(CONTENT) ||
				lowerCaseKey.equals(HTTP_HEADER_MAP) ||
				httpHeader.containsKey(lowerCaseKey));
	}

	/* (non-Javadoc)
	 * @see java.util.Map#containsValue(java.lang.Object)
	 * <b>Note:</b> If used on a binary record, like audio or image,
	 * this method returns 'false', if a match is not found in
	 * any of the metadata. No attempt is made to search through
	 * the binary content. We can't throw an error,
	 * because that breaks the Map interface contract. 
	 */
	@Override
	public boolean containsValue(Object value) {
		if (md.containsValue(value))
			return true;
		if (httpHeader.containsValue(value))
			return true;
		if (getContentType() == WebContentType.TEXT)
			return ((String) getContent()).contains((String) value);
		else {
			logger.warn("Called 'containsValue() on binary content. Did not match '" + value + "' against content itself; only against metadata.");
			return false;
		}
	}

	/* (non-Javadoc)
	 * @see java.util.Map#get(java.lang.Object)
	 * <b>Note:</b> This method only works for the metadata. You
	 * must use getContent() to retrieve content from binary records.
	 */
	@Override
	public String get(Object key) {
		String val;
		// Is the key from one of the WebBase header fields?
		if ((val = md.fieldToString((String) key)) != null)
			return val;
		// Nope, is the key from an HTTP header field?
		if ((val = httpHeader.get(key)) != null)
			return val;
		// Nope. Does caller want the HTTP header map as a whole?
		if (key.equals(HTTP_HEADER_MAP))
			return httpHeader.toString();
		// Nope. Do they want the content?
		if (key.equals(CONTENT)) {
			// We do serve content via the Map interface for text pages...
			if (getContentType() == WebContentType.TEXT) {
				return (String) getContent();
			} else {
				// ... but not for binaries:
				logger.warn("Called 'get()' to retrieve the content of a binary record. Use getContent() instead.");
			}
		}
		return null;
	}
	
	/* (non-Javadoc)
	 * @see java.util.Map#put(java.lang.Object, java.lang.Object)
	 * All key/values are added to the metadata record.
	 */
	public String put(String key, Object value) {
		String lowerCaseKey = key.toLowerCase();
		return (md.put(lowerCaseKey, value)).toString();
	}

	/* (non-Javadoc)
	 * @see java.util.Map#remove(java.lang.Object)
	 * <b>Note</b>If called to remove the CONTENT value on a binary record
	 * the return value is a String whose content is the old byte array.
	 * I'm not sure whether this 
	 * 
	 */
	@Override
	public String remove(Object key) {
		String lowerCaseKey = ((String)key).toLowerCase();
		Object oldVal;
		if ((oldVal = md.remove(lowerCaseKey)) != null)
			return oldVal.toString();
		if ((oldVal = httpHeader.remove(lowerCaseKey)) != null)
				return oldVal.toString();
		if (key == HTTP_HEADER_MAP) {
			logger.error("Attempt to delete HTTP header from record. Can't be done.");
			return null;
		}
		if (key == CONTENT) {
			if (getContentType() == WebContentType.TEXT) {
				String oldContent = (String) getContent();
				wbContent = new byte[0];
				return oldContent;
			}
			else {
				logger.warn("Used remove() to delete content from binary record. Content was removed, but old content was not returned.");
				wbContent = new byte[0];
				return null;
			}
		}
		return null;
	}

	public Collection<Object> values() {
		Collection<Object> objVals = md.values();
		// Add the HTTP header vals flat:
		objVals.add(httpHeader);
		objVals.add(getContent());
		return objVals;
	}
	
	@Override
	public Set<String> keySet() {
		Set<String> res = md.keySet();
		res.addAll(httpHeader.keySet());
		res.add(CONTENT);
		return res;
	}
	
	/* (non-Javadoc)
	 * @see pigir.webbase.WbRecordMap#keySetHeader()
	 * Return the keys of non-content fields. This
	 * includes the header fields in every WebBase 
	 * record, plus the HTTP fields that were present in
	 * this record's HTTP header. All the respective
	 * values are relatively short.
	 */
	public Set<String> keySetHeader() {
		Set<String> allKeys = md.keySet();
		allKeys.addAll(httpHeader.keySet());
		return allKeys;
	}
	
	/* (non-Javadoc)
	 * @see pigir.webbase.WbRecordMap#valuesHeader()
	 * Return the values of the non-content header fields.
	 */
	public Collection<Object> valuesHeader() {
		Collection<Object> coll = new HashSet<Object>();
		for (String key : keySetHeader()) {
			coll.add(get(key));
		}
		return coll;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Set entrySet() {
		return entrySet(true);
	}
	
	public Set<Entry<String,Object>> entrySet(boolean readContent) {
		//Set<Entry> res = new HashSet<Entry>();
		HashSet<Entry<String,Object>> res = new HashSet<Entry<String,Object>>();
		
		for (Map.Entry<String, Object> mdEntry : md.entrySet()){
			res.add(new Entry<String,Object>(mdEntry.getKey(), mdEntry.getValue()));
		}
		if (readContent) {
			res.add(new Entry<String,Object>(CONTENT, getContent()));
		}
			
		return res;
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		for (String key : keySet()) {
			put(key, m);
		}
	}
	
	//--------------------------------------------   Inner Class Entry ----------------------
	protected class Entry<K,V> implements Map.Entry<K,V> {

		K key;
		V value;
		
		public Entry(K theKey, V theValue) {
			key = theKey;
			value = theValue;
		}
		
		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
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

