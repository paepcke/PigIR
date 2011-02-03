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

public abstract class WbRecord extends Text implements WbRecordMap {
	
	public static final boolean INCLUDE_CONTENT = true; 
	public static final boolean DONT_INCLUDE_CONTENT = false; 
	
	// All lower-case WebBase header field names:
	public static final String WEBBASE_URL = "webbase-url";
	public static final String WEBBASE_DATE = "webbase-date";
	public static final String WEBBASE_POSITION = "webbase-position";
	public static final String WEBBASE_DOCID = "webbase-docid";
	public static final String CONTENT = "content";
	
	
	private Metadata md;
	private Vector<String> httpHeader;
	protected byte[] content;

	// Instance variables:
	protected HashMap<String,String> headerMap = null;
	private byte[] wbContent=null;
	
	// Lookup table for properly capitalized WebBase header fields:
	// Used wbRecordReader toString();
	/*
	@SuppressWarnings("serial")
	private static final Map<String, String> WEBBASE_HEADER_FIELD_NAMES = new HashMap<String, String>() {
		{
			put(WEBBASE_URL, "WebBase-URL");
			put(WEBBASE_DATE, "WebBase-Date");
			put(WEBBASE_POSITION, "WebBase-Position");
			put(WEBBASE_DOCID, "WebBase-DOCID");
		}
	};
	*/
	
	private static final String[] mandatoryHeaderFields = {WEBBASE_URL,
														   WEBBASE_DATE,
														   WEBBASE_POSITION,
														   WEBBASE_DOCID
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
	public HashMap<String,Constructor> mandatoryWbHeaderFldTypes = new HashMap<String, Constructor>() {
		{
			put(WEBBASE_URL, strConstructor);
			put(WEBBASE_DATE, strConstructor);
			put(WEBBASE_POSITION, intConstructor);
			put(WEBBASE_DOCID, intConstructor);
		}
	};
	
	public abstract WebContentType getContentType();

	public WbRecord(Metadata md, String httpHeader, byte[] content){
		this(md, WbRecordFactory.getHTTPHeader(httpHeader), content);
	}
	
	public WbRecord(Metadata md, Vector<String> httpHeader, byte[] content) {
		this.md = md;
		this.content = content;
		this.httpHeader = httpHeader;
	}
	
	public abstract Object getContent();
	
	protected void setContent(byte[] c) {
		this.content = c;
	}
	
	public Metadata getMetadata() {
		return md;
	}
	
	protected void setMetadata(Metadata d) {
		this.md = d;
	}
	
	public Vector<String> getHTTPHeaderAsVector() {
		return httpHeader;
	}
	
	public String getHTTPHeaderAsString() {
		String retString = "";
		for(String s : this.httpHeader) {
			retString += s;
			retString += "\r\n";
		}
		retString += "\r\n";
		return retString;
	}
	
	protected void setHTTPHeader(Vector<String> header) {
		this.httpHeader = header;
	}
	
	protected void setHTTPHeader(String header) {
		this.httpHeader = WbRecordFactory.getHTTPHeader(header);
	}
	
	public String toString () {
		return toString(DONT_INCLUDE_CONTENT);
	}
	
	public String toString(Boolean includeContent) {
		if (includeContent)
			return md.toString() + getHTTPHeaderAsString() + (new String(this.content));
		else
			return md.toString() + getHTTPHeaderAsString();
	}
	
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
	
	public String[] mandatoryKeysHeader() {
		return mandatoryHeaderFields;
	}
	
	public String[] mandatoryValuesHeader() {
		String[] res = new String[mandatoryHeaderFields.length];
		for (int i=0; i<mandatoryHeaderFields.length; i++) {
			res[i] = get(mandatoryHeaderFields[i]);
		}
		return res;
	}
	
	//  -----------------------------------  MAP<String,String> Methods -----------------------

	@Override
	public int size() {
		// Plus 1 is for the pseudo 'content' byte array
		// that's not really part of the hash:
		return headerMap.size() + 1;
	}

	@Override
	public boolean isEmpty() {
		return headerMap.isEmpty() && (wbContent.length == 0); 
	}

	@Override
	public boolean containsKey(Object key) {
		String lowerCaseKey = ((String) key).toLowerCase();
		return (headerMap.containsKey(lowerCaseKey) || lowerCaseKey.equals(CONTENT));
	}

	@Override
	public boolean containsValue(Object value) {
		if (headerMap.containsValue(value))
			return true;
		String content = getContentUTF8();
		return content.contains((String) value);
	}

	@Override
	public String get(Object key) {
		if (((String) key).equalsIgnoreCase(CONTENT)) {
			return getContentUTF8();
		}
		return headerMap.get(((String)key).toLowerCase());
	}

	@Override
	public String put(String key, String value) {
		String prevValue;
		String lowerCaseKey = key.toLowerCase();
		if (lowerCaseKey.equals(CONTENT)) {
			prevValue = getContentUTF8();
			wbContent = value.getBytes();
			return prevValue;
		}
		prevValue = headerMap.get(lowerCaseKey);
		headerMap.put(lowerCaseKey, value);
		return prevValue;
	}
	
	@Override
	public String remove(Object key) {
		String prevValue;
		String lowerCaseKey = ((String)key).toLowerCase();
		if (lowerCaseKey.equalsIgnoreCase(CONTENT)) {
			prevValue = getContentUTF8();
			wbContent = new byte[0];
			return prevValue;
		}
		return headerMap.remove(lowerCaseKey);
	}

	@Override
	public void putAll(Map<? extends String, ? extends String> m) {
		for (String key : m.keySet()) {
			put(key, m.get(key));
		}
	}

	@Override
	public Set<String> keySet() {
		Set<String> res = headerMap.keySet();
		res.add(CONTENT);
		return res;
	}
	
	public Set<String> keySetHeader() {
		return headerMap.keySet();
	}
	
	@Override
	public Collection<String> values() {
		Collection<String> res = headerMap.values();
		res.add(getContentUTF8());
		return res;
	}
	
	@Override
	public Collection<String> valuesHeader() {
		return headerMap.values();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
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

