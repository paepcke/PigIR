/* WebStream
     Written by Alexander Bonomo

 WbTextRecord class

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

import java.util.Vector;

public class WbTextRecord extends WbRecord  {
	public WbTextRecord(Metadata md, String httpHeader, byte[] content) {
		super(md, httpHeader, content);
	}	

	public WbTextRecord(Metadata md, Vector<String> httpHeader, byte[] content) {
		super(md, httpHeader, content);
	}

	@Override
	public String getContent() {
		return new String(this.content);
	}

	@Override
	public WebContentType getContentType() {
		return WebContentType.TEXT;
	}
}
