/* WebStream
     Written by Alexander Bonomo

 WebContentFactory class

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Vector;

public class WbRecordFactory {
	
	public static WbRecord getWbRecord(Metadata md, byte[] page) {
		
		// Get the page as a string to extract the http header
		String pageStr = new String(page);
		Vector<String> httpHeader = getHTTPHeader(pageStr);
		
		//determine the web content type from the http header
		WebContentType type = getContentType(httpHeader);
		
		//get the content of the page without the http header
		byte[] content = pageStr.substring(pageStr.indexOf("\r\n\r\n") + 4).getBytes();
		
		switch(type) {
		case TEXT:
			return new WbTextRecord(md, httpHeader, content);
			
		case AUDIO:
			return new WbAudioRecord(md, httpHeader, content);
		
		case IMAGE:
			return new WbImageRecord (md, httpHeader, content);
			
		default:
			return new WbDefaultRecord (md, httpHeader, content);
		}
	}
	
	// Right now this assumes there is exactly 1 HTTP header as the first part of every page
	static Vector<String> getHTTPHeader(String page) {
		Vector<String> httpHeader = new Vector<String>();
		BufferedReader reader = new BufferedReader(new StringReader(page));
		String currentLine;
		while(true) {
			try {
				currentLine = reader.readLine();
				
				//break if we reach the end of the page or end of the header
				if(currentLine == null)
					break;
				if(currentLine.equalsIgnoreCase(""))
					break;
				
				httpHeader.add(currentLine);
			} 
			catch (IOException e) {
				break;
			}
		}
		return httpHeader;
	}
	
	private static WebContentType getContentType(Vector<String> httpHeader) {
		for(String s : httpHeader) {
			String[] line = s.split(":", 2);
			if(line[0].equals("Content-Type")) {
				String[] type = line[1].trim().split("/");
				try {
					return WebContentType.valueOf(type[0].toUpperCase());
				} catch(IllegalArgumentException e) {
					return WebContentType.DEFAULT;
				}
			}
		}
		return WebContentType.DEFAULT;
	}
}
