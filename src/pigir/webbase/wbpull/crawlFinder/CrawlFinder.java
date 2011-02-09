/* CrawlFinder
     Written by Alexander Bonomo

 parse command line
 build a request
 get a crawl entry
 start a web stream

 args:
 crawl name
 crawl mime/type
 start page (optional)
 end page (optional)
 offset (optional)
 ip address (optional)
 port (optional)

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

package pigir.webbase.wbpull.crawlFinder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import pigir.webbase.wbpull.CrawlEntry;
import pigir.webbase.wbpull.CrawlRequest;
import pigir.webbase.wbpull.handler.CatHandler;
import pigir.webbase.wbpull.webStream.WebStream;
import pigir.webbase.wbpull.webStream.WebStreamCallback;
import pigir.webbase.wbpull.webStream.WebStreamIterator;
import pigir.webbase.wbpull.webStream.WebStreamType;


public class CrawlFinder {
	
	static final int DIRECTORY_READ_TIMEOUT = 5000; // time in msecs.
	
	static Logger logger = null;
	//program arguments
	private static final String nameArg			= "-name";
	private static final String typeArg			= "-type";
	private static final String offsetArg		= "-offset";
	private static final String startSiteArg	= "-start";
	private static final String endSiteArg		= "-end";
	private static final String ipArg			= "-ip";
	private static final String portArg			= "-port";
	
	//argument values
	private static String crawlName		= null;
	private static String crawlType 	= null;
	private static String startSite 	= null;
	private static String endSite 		= null;
	private static String ipAddress		= "127.0.0.1";
	private static int port				= 3000;
	private static int offset 			= 0;	
	
	public static void main(String[] args) {

		logger = Logger.getLogger(CrawlFinder.class.getName());
		//PropertyConfigurator.configure("conf/log4j.properties");
	
		//process arguments
		//processArguments(args);
		processArguments(new String[] {"-name:crawled_hosts.1208", "-type:text"});
		
		//ensure at least name and type arguments were supplied
		if(crawlName == null || crawlType == null) {
			logger.error("Either crawl name or crawl type not specified. Now exiting.");
			usage();
			return;
		}
		
		CrawlEntry entry = FindCrawl(crawlName, crawlType);
		
		if(entry == null) return;
		
		logger.info("Get stream from " + entry.getMachineName() + ".stanford.edu:" + entry.getPort());
		
		WebStreamCallback ws = (WebStreamCallback)GetWebStream(entry, WebStreamType.CALLBACK);
		
		if (ws == null) {
			logger.error("Web page stream is null. Maybe crawl not found?");	
			return;
		}
		
		logger.info(ws.getMachineName() + ":" + ws.getPort());
		
		/*for(int i = 0; i < 2; i++) {
			if(ws.hasNext())
				System.out.println(ws.next());
		}*/
		
		CatHandler ch = new CatHandler();		
		ws.registerCallback(ch);
		ws.startStream();
	}
	
	public static CrawlEntry FindCrawl(String name, String mime) {
		
		CrawlEntry result;
		//create a request to send to the directory service:
		CrawlRequest cr = new CrawlRequest();
		cr.setCrawlName(name);
		cr.setCrawlMime(mime);
		
		try {
			//set up socket and input/output streams
			Socket dir_serv = new Socket(ipAddress, port);//TODO: change to eventual crawl lookup server on WBxx
			dir_serv.setSoTimeout(DIRECTORY_READ_TIMEOUT);
			ObjectOutputStream oos = new ObjectOutputStream(dir_serv.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(dir_serv.getInputStream());
			
			//send the request
			oos.writeObject(cr);
			oos.flush();
			
			//get the entry
			try {
				result = (CrawlEntry) ois.readObject();
			} catch (Exception e) {
				String errMsg = "Could not obtain machine name and port of distributor for crawl " +
						name + ":" + mime; 
				logger.error(errMsg);
				throw new TimeoutException(errMsg);
			}
			
			//close input/output streams and the socket
			oos.close();
			ois.close();
			dir_serv.close();
			
			return result;
		}
		catch(Exception e) {
			return null;
		}
	}
	
	public static WebStream GetWebStream(CrawlEntry entry, WebStreamType type) {
		try {
			//open socket to machine defined by entry to figure out where the data is
			String	host	= entry.getMachineName() + ".stanford.edu";
			int 	port	= Integer.parseInt(entry.getPort().trim()); 
			Socket	remote	= new Socket(host, port);
			
			//send request
			String remoteLine = "new," + offset;
			if(CrawlFinder.startSite != null) {
				remoteLine += "," + startSite;
				if(CrawlFinder.endSite != null)
					remoteLine += "," + endSite;
			}
			remoteLine += ",\015\012";
			
			PrintWriter out = new PrintWriter(remote.getOutputStream());
			out.print(remoteLine);
			out.flush();
			
			//get response
			BufferedReader in = new BufferedReader(new InputStreamReader(remote.getInputStream()));
			String response = in.readLine();
			
			//close input/output streams and socket
			out.close();
			in.close();
			remote.close();
			
			//parse response and return corresponding WebStream
			String[] responseArr = response.split(" ");//NOTE: response = "ip_addr port"			
			
			switch(type) {
			case ITERATOR:
				return new WebStreamIterator(responseArr[0], responseArr[1], entry.getNumPages());
			case CALLBACK:
				return new WebStreamCallback(responseArr[0], responseArr[1], entry.getNumPages());
			default:
				return null;	
			}
			
		}
		catch(Exception e) {
			return null;
		}
	}

	public static void setOffset(int offset) {
		CrawlFinder.offset = offset;
	}
	public static void setStartSite(String startPage) {
		CrawlFinder.startSite = startPage;
	}

	public static void setEndPage(String endPage) {
		CrawlFinder.endSite = endPage;
		}
	
	private static void processArguments(String[] args) {
		for(int i = 0; i < args.length; i++) {
			String[] curArg = args[i].split(":", 2);
			
			//check for crawl name
			if(curArg[0].equalsIgnoreCase(nameArg)) {
				if(curArg.length < 2) continue;
				crawlName = curArg[1];
			}
			
			//check for crawl mime/type
			else if(curArg[0].equalsIgnoreCase(typeArg)) {
				if(curArg.length < 2) continue;
				crawlType = curArg[1];
			}
			
			//check for offset
			else if(curArg[0].equalsIgnoreCase(offsetArg)) {
				if(curArg.length < 2) continue;
				try {
					offset = Integer.parseInt(curArg[1]);
				}
				catch(NumberFormatException e) {
					System.out.println("Couldn't parse offset. Ignoring argument.");
				}
			}
			
			//check for start page
			else if(curArg[0].equalsIgnoreCase(startSiteArg)) {
				if(curArg.length < 2) continue;
				startSite = curArg[1];
			}
			
			//check for end page
			else if(curArg[0].equalsIgnoreCase(endSiteArg)) {
				if(curArg.length < 2) continue;
				endSite = curArg[1];
			}
			
			//check for ip
			else if(curArg[0].equalsIgnoreCase(ipArg)) {
				if(curArg.length < 2) continue;
				ipAddress = curArg[1];
			}
			
			//check for port
			else if(curArg[0].equalsIgnoreCase(portArg)) {
				if(curArg.length < 2) continue;
				try {
					port = Integer.parseInt(curArg[1]);
				}
				catch(NumberFormatException e) {
					System.out.println("Couldn't parse port argument. Reverting to default.");
				}
			}
			
			//unrecognized argument
			else {
				System.out.println("Unrecognized argument: " + args[i]);
				usage();
			}
		}
	}
	
	private static void usage() {
		System.out.println("\nUsage:");
		System.out.println("CrawlFinder -name:crawlName -type:crawlMime [-start:startPage] [-end:endPage] " 
				+ "[-offset:offset] [-ip:ipAddress] [-port:portNumber]\n");
	}
}
