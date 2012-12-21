/* WebStream
     Written by Alexander Bonomo

 CatHandler class

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

package edu.stanford.pigir.webbase.wbpull.handler;

import edu.stanford.pigir.webbase.WbRecord;

public class CatHandler extends Handler 
{
	public CatHandler()
	{
		super();
	}
	
	@Override
	public void run()
	{
		WbRecord current;
		while(!isStreamFinished())
		{
			try 
			{
				//just grab the next page and spit it out
				current = this.contentQueue.take();
				System.out.println(current);
			} 
			catch (InterruptedException e) 
			{
				//this probably means the stream is finished, so just continue and check
				continue;
			}
		}
	}
}
