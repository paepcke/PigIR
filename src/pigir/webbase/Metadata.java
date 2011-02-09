/* WebStream
     Written by Alexander Bonomo

 Metadata class

 The Stanford WebBase Project <webbase db stanford edu>
 Copyright (C) 2010 The Board of Trustees of the
 Leland Stanford Junior University

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License
 as published by the Free Software Foundation; either version 2
 of the License, or (at your option) any later version.
  This program is distributed wbRecordReader the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
  You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/  

package pigir.webbase;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * Holds Metadata for one Web page.
 * 
 * @author paepcke
 *
 */
public class Metadata implements Map<String,Object> {

	private static final long serialVersionUID = 1L;
	private static Logger logger = null;
	private static HashSet<String> keys = new HashSet<String>() {
		private static final long serialVersionUID = 1L;
		{
			add(WbRecord.WEBBASE_URL);
			add(WbRecord.WEBBASE_DATE);
			add(WbRecord.WEBBASE_SIZE);
			add(WbRecord.WEBBASE_POSITION);
			add(WbRecord.WEBBASE_DOCID);
		}
	} ;
	private static int NUM_METADATA_ITEMS = keys.size();
	
	int docID;
	long offset;
	int pageSize;
	String timeStamp;
	String url;
	
	public Metadata(int docID, int thePageSize, long offest, String timeStamp, String url) {
		this.docID = docID;
		this.timeStamp = timeStamp;
		this.url = url;
		this.pageSize = thePageSize;
		if (logger == null)
			logger = WbRecordReader.getLogger();
	}
	
	public int size() {
		return NUM_METADATA_ITEMS;
	}
	
	public int getDocID() {
		return docID;
	}

	public long getOffset() {
		return offset;
	}

	public long getPageSize() {
		return pageSize;
	}

	public String getTimeStamp() {
		return timeStamp;
	}

	public String getURLAsString() {
		return url;
	}
	
	public URL getURLAsURL(){
		try {
			return new URL(url);
		} catch (MalformedURLException e) {
			return null;
		}
	}
	
	public String toString() {
		return 
		"URL: " + url + "\r\n"
		+ "Date: " + timeStamp + "\r\n"
		+ "Length: " + pageSize + "\r\n"		
		+ "Position: " + offset + "\r\n"
		+ "DocId: " + docID + "\r\n";
	}
	
	// ------------------------------------------  Methods for implementing Map<String,String> ------------

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public boolean containsKey(Object key) {
		return keys.contains(key);
	}

	@Override
	public boolean containsValue(Object value) {
		if (value.equals(url)) return true;
		if (value.equals(timeStamp)) return true;
		if (value.equals(pageSize)) return true;
		if (value.equals(docID)) return true;
		if (value.equals(offset)) return true;
		return false;
	}

	@Override
	public Object get(Object key) {
		if (key.equals(WbRecord.WEBBASE_URL)) return url;
		if (key.equals(WbRecord.WEBBASE_DATE)) return timeStamp;
		if (key.equals(WbRecord.WEBBASE_SIZE)) return pageSize;
		if (key.equals(WbRecord.WEBBASE_POSITION)) return offset;
		if (key.equals(WbRecord.WEBBASE_DOCID)) return docID;
		return null;
	}
	
	@Override
	public Object put(String key, Object value) {
		Object prevValue = null;
		if (key.equals(WbRecord.WEBBASE_URL)) {
			prevValue = url;
			url = (String) value;
		}
		if (key.equals(WbRecord.WEBBASE_DATE)) {
			prevValue = timeStamp;
			timeStamp = (String) value;
		}
		if (key.equals(WbRecord.WEBBASE_SIZE)) {
			prevValue = pageSize;
			pageSize = (Integer) value;
		}
		if (key.equals(WbRecord.WEBBASE_POSITION)) {
			prevValue = offset;
			offset = (Long) value;
		}
		if (key.equals(WbRecord.WEBBASE_DOCID)) {
			prevValue = docID;
			docID = (Integer) value;
		}
		return prevValue;
	}

	@Override
	public Object remove(Object key) {
		// Don't really remove the value.
		return get(key);
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> otherMap) {
		for (String key : otherMap.keySet()) {
			put(key, otherMap.get(key));
		}
	}

	@Override
	public void clear() {
		// we'll do no such thing.
	}

	@Override
	public Set<String> keySet() {
		return keys;
	}

	@Override
	public Collection<Object> values() {
		HashSet<Object> res = new HashSet<Object>();
		for (String key : keys) {
			res.add(get(key));
		}
		return res;
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		HashSet<java.util.Map.Entry<String, Object>> res = new HashSet<java.util.Map.Entry<String, Object>>(); 
		for (String key : keys) {
			res.add(new Entry<String,Object>(key, get(key)));
		}
		return res;
	}
	
	@SuppressWarnings("hiding")
	class Entry<String, Object> implements Map.Entry<String, Object> {

		String key;
		Object value;
		
		public Entry(String theKey, Object theVal) {
			key = theKey;
			value = theVal;
		}
		
		@Override
		public String getKey() {
			return key;
		}

		@Override
		public Object getValue() {
			return value;
		}

		@SuppressWarnings("unchecked")
		@Override
		public java.lang.Object setValue(java.lang.Object val) {
			Object oldVal = value;
			value = (Object) val;
			return oldVal;
		}
	}
}
