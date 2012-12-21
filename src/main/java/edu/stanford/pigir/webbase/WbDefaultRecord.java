/* WebStream
     Written by Alexander Bonomo

 WbDefaultRecord class

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

package edu.stanford.pigir.webbase;

import java.util.Vector;

@SuppressWarnings("unchecked")
public class WbDefaultRecord extends WbBinaryRecord { 
	
	public WbDefaultRecord(Metadata md, String httpHeader, byte[] content) {
		super(md, httpHeader, content);
	}
	
	public WbDefaultRecord(Metadata md, Vector<String> httpHeader, byte[] content) {
		super(md, httpHeader, content);
	}

	@Override
	public WebContentType getContentType() {
		return WebContentType.DEFAULT;
	}
	
	public String getContentAsString() {
		return new String(this.wbContent);
	}
}
