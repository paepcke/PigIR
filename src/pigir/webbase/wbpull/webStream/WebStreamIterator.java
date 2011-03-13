/* WebStream
     Written by Alexander Bonomo

 WebStreamIterator class

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

import java.util.Iterator;

import pigir.webbase.WbRecord;


public class WebStreamIterator extends WebStream implements Iterator<WbRecord> {
	private WbRecord currentPage = null;
	
	public WebStreamIterator(String ip, int port, int totalNumPages)  {
		super(ip, port, totalNumPages);
	}

	public WebStreamIterator(String ip, String port, int totalNumPages) {
		super(ip, port, totalNumPages);
	}

	@Override
	public boolean hasNext() {
		if(currentPage != null)
			return true;
		
		currentPage = next();
		return currentPage != null;
	}

	@Override
	public WbRecord next() {
		if(currentPage != null) {
			WbRecord temp = currentPage;
			currentPage = null;
			return temp;
		}
		return this.getNPages(1).get(0);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
