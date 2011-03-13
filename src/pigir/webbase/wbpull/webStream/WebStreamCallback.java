/* WebStream
     Written by Alexander Bonomo

 WebStreamCallback class

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

import java.util.Vector;

import pigir.webbase.WbRecord;
import pigir.webbase.wbpull.handler.Handler;

public class WebStreamCallback extends WebStream 
{
	private WbRecord currentPage = null;	
	private Vector<Handler> callbacks = new Vector<Handler>();
	
	public WebStreamCallback(String ip, int port, int totalNumPages) 
	{
		super(ip, port, totalNumPages);
	}

	public WebStreamCallback(String ip, String port, int totalNumPages) 
	{
		super(ip, port, totalNumPages);
	}

	//NOTE: vectors are synchronized, but if funny things start to happen, synchronize this method too
	public void registerCallback(Handler h)
	{
		this.callbacks.add(h);
	}
	
	public void startStream()
	{
		WbRecord page;
		
		//pass the current page to each of the call backs
		while(true)
		{
			//get the next page, if it is null, then we are done w/ the stream so break out of the loop
			if(currentPage != null)
				page = currentPage;
			else
				page = this.getNPages(1).get(0);
			
			if(page == null)
				break;
			
			for(Handler h : this.callbacks)
			{
				h.addToQueue(page);
			}
		}
		
		//tell each of the callbacks that the stream is finished
		for(Handler h : this.callbacks)
			h.streamFinished();
	}
	
	public boolean hasNext()
	{
		if(currentPage != null)
			return true;
		
		currentPage = this.getNPages(1).get(0);
		return currentPage != null;
	}
}
