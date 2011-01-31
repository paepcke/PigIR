/* CrawlRequest
     Written by Alexander Bonomo

 CrawlRequest class

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

public class CrawlRequest implements Serializable 
{
	private static final long serialVersionUID = 821005648236454987L;
	private String crawlName;
	private String crawlMime;
	
	public CrawlRequest()
	{
		//...
	}

	public String getCrawlName() 
	{
		return crawlName;
	}

	public String getCrawlMime() 
	{
		return crawlMime;
	}

	public void setCrawlName(String crawlName) 
	{
		this.crawlName = crawlName;
	}

	public void setCrawlMime(String crawlMime) 
	{
		this.crawlMime = crawlMime;
	}
	
	
}
