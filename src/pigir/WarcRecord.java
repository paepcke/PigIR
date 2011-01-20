package pigir;

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
 *    nor may "Lemur" or "Indri" appear in their names without prior written
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
 * Jan 19, 2011; Andreas Paepcke: modified to fit in Hadoop/Pig workflow. 
 *                                Replaced separate header API with a 
 *                                Map<String,String> implementation that
 *                                includes 'content' as one of its fields.
 * 
 */

import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.Text;

public class WarcRecord extends Text implements WarcRecordMap {

	// Class variables:
	
	public static final String CONTENT = "content";

	/*
	public static final String WARC_TYPE = "WARC-Type";
	public static final String WARC_RECORD_ID = "WARC-Record-ID";
	public static final String WARC_DATE = "WARC-Date";
	public static final String CONTENT_LENGTH = "Content-Length";
	public static final String CONTENT_LENGTH_LOWER_CASE = "content-length";  // Speed up loop below.
	public static final String CONTENT_TYPE = "Content-Type";
	public static final String WARC_CONCURRENT_TO = "WARC-Concurrent-To";
	public static final String WARC_BLOCK_DIGEST = "WARC-Block-Digest";
	public static final String WARC_PAYLOAD_DIGEST = "WARC-Payload-Digest";
	public static final String WARC_IP_ADDRESS = "WARC-IP-Address";
	public static final String WARC_REFERS_TO = "WARC-Refers-To";
	public static final String WARC_TARGET_URI = "WARC-Target-URI";
	public static final String WARC_TRUNCATED = "WARC-Truncated";
	public static final String WARC_WARCINFO_ID = "WARC-Warcinfo-ID";
	public static final String WARC_FILENAME = "WARC-Filename";
	public static final String WARC_PROFILE = "WARC-Profile";
	public static final String WARC_IDENTIFIED_PAYLOAD_TYPE = "WARC-Identified-Payload-Type";
	public static final String WARC_SEGMENT_ORIGIN_ID = "WARC-Segment-Origin-ID";
	public static final String WARC_SEGMENT_NUMBER = "WARC-Segment-Number";
	public static final String WARC_SEGMENT_TOTAL_LENGTH = "WARC-Segment-Total-Length";
	*/
	
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
	// Fast method for looking up whether a header key is mandatory or not:
	@SuppressWarnings("serial")
	private static final HashMap<String,Boolean> mandatoryHeaderFieldsLookup = new HashMap<String, Boolean>() {
		{
			for (String key : mandatoryHeaderFields) {
				put(key, true);
			}
		}
	};
	
	
	public static String WARC_VERSION = "WARC/0.18";
	public static String WARC_VERSION_LINE = "WARC/0.18\n";
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
	 * The actual heavy lifting of reading in the next WARC record
	 * 
	 * @param warcLineReader a line reader
	 * @return the content bytes (w/ the headerBuffer populated)
	 * @throws java.io.IOException
	 */
	private static byte[] readNextRecord(LineAndChunkReader warcLineReader) throws IOException {
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
		// is in the header, because we rely on it below. 
		// We do not check for the other mandatory header fields:
		int contentLength = pullHeaderFromStream(warcLineReader, 
												 txtBuf);
		txtBuf.clear();
		
		if (contentLength < 0) {
			return null;
		}

		// Pull the bytes of the content from the stream:
		retContent=new byte[contentLength];
		int totalWant=contentLength;
		int totalRead=0;
		while (totalRead < contentLength) {
			try {
				int numRead=warcLineReader.read(retContent, totalRead, totalWant);
				//************
				System.out.println(new String(retContent));
				//************
				if (numRead < 0) {
					return null;
				} else {
					totalRead += numRead;
					totalWant = contentLength-totalRead;
				} // end if (numRead < 0) / else
			} catch (EOFException eofEx) {
				// resize to what we have
				if (totalRead > 0) {
					byte[] newReturn=new byte[totalRead];
					System.arraycopy(retContent, 0, newReturn, 0, totalRead);
					tmpGrandTotalBytesRead += totalRead;
					return newReturn;
				} else {
					return null;
				}
			} // end try/catch (EOFException)
		} // end while (totalRead < contentLength)

		tmpGrandTotalBytesRead += totalRead;
		return retContent;
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
					headerAttrName  = thisHeaderPieceParts[0].toLowerCase();
					headerAttrValue =  thisHeaderPieceParts[1];
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
	 * @param foundMark
	 * @return
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
			if (line.startsWith(WARC_VERSION)) {
				foundMark=true;
			}
			txtBuf.clear();
		}
		return foundMark;
	}

	/**
	 * Reads in a WARC record from a data input stream
	 * @param in the input stream
	 * @return a WARC record (or null if eof)
	 * @throws java.io.IOException
	 */
	@SuppressWarnings("unchecked")
	public static WarcRecord readNextWarcRecord(LineAndChunkReader warcInLineReader) throws IOException {
		
		byte[] recordContent=readNextRecord(warcInLineReader);
		if (recordContent==null) { 
			return null; 
		}

		WarcRecord retRecord=new WarcRecord();
		retRecord.headerMap = (HashMap<String, String>) tmpHeaderMap.clone();
		retRecord.grandTotalBytesRead = tmpGrandTotalBytesRead;
		retRecord.optionalHeaderKeysThisRecord = (HashSet<String>) tmpOptionalHeaderKeys.clone();
		retRecord.setRecordContent(recordContent);
		
		//************
		System.out.println(new String(recordContent));
		//************

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
		StringBuffer retBuffer=new StringBuffer();
		//***********retBuffer.append(warcHeader.toString());
		retBuffer.append(NEWLINE);
		retBuffer.append(warcContent);
		return retBuffer.toString();
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
		return headerMap.isEmpty() && (warcContent.length == 0); 
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
			warcContent = value.getBytes();
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
			warcContent = new byte[0];
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

	@Override
	public Set<java.util.Map.Entry<String, String>> entrySet() {
		throw new NotImplementedException("Method 'entrySet' is not implemented on WarcRecord");
	}
}

