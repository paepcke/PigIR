/* WebStream
     Written by Alexander Bonomo

 WebStream class

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

package pigir.webbase.wbpull.webStream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Vector;

import pigir.Common;
import pigir.webbase.Metadata;
import pigir.webbase.WbRecord;
import pigir.webbase.WbRecordFactory;

public abstract class WebStream
{
	//constants
	protected static final int TIMESTAMP_LENGTH = 24;
	protected static final int MAX_URL_SIZE = 100*1024;
	protected static final int MAX_WEBPAGE_SIZE = 100 * 1024 * 1024;
	
	protected String machineName;
	protected int port;
	
	private Integer offset = null;
	
	private int numPagesRequested; //total number of pages wbRecordReader is to pull
	protected int totalNumPagesRetrieved; // let subclasses update this directly
	
	public WebStream(String ip, int port, int totalNumPages)
	{
		this.machineName = ip;
		this.port = port;
		this.numPagesRequested = totalNumPages;
		this.totalNumPagesRetrieved = 0;
	}
	
	public WebStream(String ip, String port, int totalNumPages)
	{
		this(ip, Integer.parseInt(port), totalNumPages);
	}
	
	public String getMachineName() {
		return machineName;
	}

	public int getPort() {
		return port;
	}
	
	public int getOffset() {
		return this.offset.intValue();
	}
	
	public void setOffset(int n) {
		this.offset = Integer.valueOf(n);
	}

	public int getNumPagesRequested() {
		return this.numPagesRequested;
	}
	
	public int getNumPagesRetrieved() {
		return this.totalNumPagesRetrieved;
	}
	
	public Vector<WbRecord> getAllPages() {
		// Not a realistic method for large crawls!
		Vector<WbRecord> content = getNPages(130000000);
		System.out.println("Number of pages expected:	" + this.numPagesRequested);
		System.out.println("Actual number of pages wbRecordReader crawl:	" + this.totalNumPagesRetrieved);
		return content;
	}
	
	public Vector<WbRecord> getNPages(int n) {
		Vector<WbRecord> pages = new Vector<WbRecord>();
		
		//socket info
		Socket				distributor;
		DataInputStream		in;
		DataOutputStream	out;
		
		//data received
		@SuppressWarnings("unused") 
		char	incomingDocType; //keep this here because we read it from the connection, even though we don't really use it 
		int		docID, pageSize, urlLen, tsLen;
		long	offset;
		//String page; //NOTE: page variable exists only for debugging purposes
		String	timeStamp, url;
		byte[] tsBytes, urlBytes, pageBytes;
		
		Metadata metadata;
		
		try {
			//set up socket and input/output streams
			distributor = Common.getSocket(machineName, port);
			in	= new DataInputStream(distributor.getInputStream());
			out	= new DataOutputStream(distributor.getOutputStream());
			
			//request pages
			out.writeInt(n);
			
			//read incoming info
			incomingDocType	= (new Character((char)in.read())).charValue();
			
			for(int i = 0; i < n; i++)
			{
				try
				{
					docID			= in.readInt();
					offset			= in.readLong();
					tsLen			= in.readInt();
					urlLen			= in.readInt();
					pageSize		= in.readInt();
						
					if(tsLen > TIMESTAMP_LENGTH || urlLen > MAX_URL_SIZE)
					{
						// aybe output an error message? just continue for now...
						continue;
					}
					
					//read timestamp
					tsBytes = new byte[tsLen];
					in.read(tsBytes, 0, tsLen);
					timeStamp  = new String(tsBytes);
					
					//read url
					urlBytes = new byte[urlLen];
					in.read(urlBytes, 0, urlLen);
					url = new String(urlBytes);
					
					//construct metadata
					metadata = new Metadata(docID, pageSize, offset, timeStamp, url);
								
					//read page
					pageBytes = new byte[pageSize];
					int bytesToRead = pageSize;
					int numRead = 0; 
					while(bytesToRead > 0) {
						numRead = in.read(pageBytes, pageSize - bytesToRead, bytesToRead);
						if(numRead < 1)
							break;
						bytesToRead -= numRead;
					}
					//page = new String(pageBytes, "UTF-8");
					//page = new String(pageBytes, "ISO-8859-1");
					//page = new String(pageBytes, "US-ASCII");
					
					pages.add(WbRecordFactory.getWbRecord(metadata, pageBytes));
					this.totalNumPagesRetrieved++;
				}catch(EOFException e) {
					break;
				} catch(IOException e){
					break;
				}
			}			
			
			//close connection
			distributor.close();
			in.close();
			out.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e)	{
			e.printStackTrace();
			return null;
		}
		return pages;
	}
}
