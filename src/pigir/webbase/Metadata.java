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

public class Metadata {
	int docID;
	long offset;
	String timeStamp;
	String url;
	
	public Metadata(int docID, long offest, String timeStamp, String url) {
		this.docID = docID;
		this.timeStamp = timeStamp;
		this.url = url;
	}
	
	public int getDocID() {
		return docID;
	}

	public long getOffset() {
		return offset;
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
		+ "Position: " + offset + "\r\n"
		+ "DocId: " + docID + "\r\n";
	}
}
