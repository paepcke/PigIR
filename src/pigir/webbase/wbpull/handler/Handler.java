/* WebStream
     Written by Alexander Bonomo

 Handler class

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

package pigir.webbase.wbpull.handler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import pigir.webbase.WbRecord;


/*
 * NOTE:
 * Sub-classes should override 1 of 2 functions:
 * addToQueue(...) to execute processing of the page wbRecordReader the same thread as the WebStream
 * or
 * run() to execute wbRecordReader a new thread
 * 
 * Sub-classes should also use the take() function to remove pages from the contentQueue
 */
public abstract class Handler extends Thread 
{	
	private boolean isStreamFinished;
	
	//Queue where WebStream puts the WebContent
	protected BlockingQueue<WbRecord> contentQueue = new LinkedBlockingQueue<WbRecord>();

	public Handler()
	{
		isStreamFinished = false;
		this.start();
	}
	
	//method that WebStream calls to pass the WebContent
	public void addToQueue(WbRecord content)
	{
		this.contentQueue.add(content);
	}
	
	//method that WebStream calls when the stream is finished
	public final void streamFinished()
	{
		isStreamFinished = true;
		this.interrupt();
	}
	
	//to allow sub-classes to check whether or not the stream is finished w/o any knowledge of the stream itself
	protected final boolean isStreamFinished()
	{
		return isStreamFinished;
	}
}
