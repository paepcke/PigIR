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
	public static final String WEBBASE_SIZE = "webbase-pageSize";
	public static final String WEBBASE_POSITION = "webbase-position";
	public static final String WEBBASE_DOCID = "webbase-docid";
	public static final String CONTENT = "content";
	
	
	private Metadata md;
	private Vector<String> httpHeader;
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
	
	public WbRecord(Metadata md, Vector<String> httpHeader, byte[] content) {
		this.md = md;
		this.wbContent = content;
		this.httpHeader = httpHeader;
		/*
		this.headerMap = new HashMap<String,String>() {
				{
					add(WEBBASE_URL, md.);
				}
		};
	}
	
	private static final String[] mandatoryHeaderFields = {WEBBASE_URL,
														   WEBBASE_DATE,
														   WEBBASE_SIZE,
														   WEBBASE_POSITION,
														   WEBBASE_DOCID
	};
	*/
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
	| getHTTPHeaderAsVector()
	------------------------*/
	public Vector<String> getHTTPHeaderAsVector() {
		return httpHeader;
	}
	
	/*-----------------------------------------------------
	| getHTTPHeaderAsString()
	------------------------*/
	public String getHTTPHeaderAsString() {
		String retString = "";
		for(String s : this.httpHeader) {
			retString += s;
			retString += "\r\n";
		}
		retString += "\r\n";
		return retString;
	}
	
	/*-----------------------------------------------------
	| setHTTPHeader()
	------------------------*/
	protected void setHTTPHeader(Vector<String> header) {
		this.httpHeader = header;
	}
	
	protected void setHTTPHeader(String header) {
		this.httpHeader = WbRecordFactory.getHTTPHeader(header);
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
	| getContent() 
	------------------------*/
	/**
	 * Retrieves the bytes content as a UTF-8 string
	 * @return
	 */
	public String getContentUTF8() {
		String retString=null;
		try {
			//retString = new String(wbContent, "UTF-8");
			retString = new String(wbContent, "UTF-8");
		} catch (UnsupportedEncodingException ex) {
			retString=new String(wbContent);
		}
		return retString;
	}
	
	/*-----------------------------------------------------
	| mandatoryKeysHeader()
	------------------------*/
	public String[] mandatoryKeysHeader() {
		return mandatoryHeaderFields;
	}
	
	/*-----------------------------------------------------
	| mandatoryValuesHeader()
	------------------------*/
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
		return md.size() + 1;
	}

	@Override
	public boolean isEmpty() {
		return (md == null) || (wbContent.length == 0); 
	}

	@Override
	public boolean containsKey(Object key) {
		String lowerCaseKey = ((String) key).toLowerCase();
		return (md.containsKey(lowerCaseKey) || lowerCaseKey.equals(CONTENT));
	}

	@Override
	public boolean containsValue(Object value) {
		if (md.containsValue(value))
			return true;
		String content = getContentUTF8();
		return content.contains((String) value);
	}

	@Override
	public String get(Object key) {
		if (((String) key).equalsIgnoreCase(CONTENT)) {
			return getContentUTF8();
		}
		return (md.get(key)).toString();
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
		return (md.put(key, value)).toString();
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
		return (md.remove(lowerCaseKey)).toString();
	}

	@Override
	public void putAll(Map<? extends String, ? extends String> m) {
		for (String key : m.keySet()) {
			put(key, m.get(key));
		}
	}

	@Override
	public Set<String> keySet() {
		Set<String> res = md.keySet();
		res.add(CONTENT);
		return res;
	}
	
	public Set<String> keySetHeader() {
		return md.keySet();
	}
	
	@Override
	public Collection<String> values() {
		Collection<Object> objVals = md.values();
		HashSet<String> res = new HashSet<String>();
		for (Object item : objVals) {
			res.add(item.toString());
		}
		res.add(getContentUTF8());
		return res;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Set entrySet() {
		return entrySet(true);
	}
	
	@Override
	public Collection<String> valuesHeader() {
		return values();
	}
	
	public Set<Entry<String,String>> entrySet(boolean readContent) {
		//Set<Entry> res = new HashSet<Entry>();
		HashSet<Entry<String,String>> res = new HashSet<Entry<String,String>>();
		
		for (Map.Entry<String, Object> mdEntry : md.entrySet()){
			res.add(new Entry<String,String>(mdEntry.getKey(), mdEntry.getValue().toString()));
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

