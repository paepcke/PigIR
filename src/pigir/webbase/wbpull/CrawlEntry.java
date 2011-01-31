/* CrawlEntry
     Written by Alexander Bonomo

 CrawlEntry class

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

package pigir.webbase.wbpull;

import java.io.Serializable;

public class CrawlEntry implements Serializable {
	private static final long serialVersionUID = 3136696790226615154L;
	private String m_machineName;
	private String m_port;
	private int m_numPages;
	
	public CrawlEntry(String machine, String port, int numPages) {
		this.m_machineName = machine;
		this.m_port = port;
		this.m_numPages = numPages;
	}

	public String getMachineName() {
		return m_machineName;
	}

	public String getPort() {
		return m_port;
	}
	
	public int getNumPages() {
		return this.m_numPages;
	}
}
