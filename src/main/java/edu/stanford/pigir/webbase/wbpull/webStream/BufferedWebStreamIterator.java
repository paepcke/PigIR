package edu.stanford.pigir.webbase.wbpull.webStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.InflaterInputStream;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.log4j.Logger;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import edu.stanford.pigir.Common;
import edu.stanford.pigir.webbase.Constants;
import edu.stanford.pigir.webbase.Metadata;
import edu.stanford.pigir.webbase.WbRecord;
import edu.stanford.pigir.webbase.WbRecordFactory;
import edu.stanford.pigir.webbase.WbRecordReader;

public class BufferedWebStreamIterator extends WebStream implements Iterator<WbRecord> {

	private static int BUFFER_SIZE = 100;
	
	private static byte COMPRESSED_MASK = 2;
	private ArrayBlockingQueue<WbRecord> wbRecordQueue = new ArrayBlockingQueue<WbRecord>(BUFFER_SIZE);
	
	private	DataInputStream		in;
	private	DataOutputStream	out;
	
	private int nextReporting = Constants.WB_PAGE_LOAD_REPORT_GRANULARITY;
	
	private Logger logger = null;
	
	protected static enum RecordCounters {
		PAGES_LOADED
	};
	protected PigStatusReporter reporter = null;
	protected Counter recordCounter = null;

	private boolean streamCompressed = false;
	
	/*-----------------------------------------------------
	|  Constructors
    ------------------------*/
	
	public BufferedWebStreamIterator(String ip, String port, String startSite, String endSite, int totalNumPages) throws IOException {
		this(ip, Integer.parseInt(port), startSite, endSite, totalNumPages);
	}
	
	public BufferedWebStreamIterator(String machineName, int distribDemonPort, String startSite, String endSite, int totalNumPages) throws IOException {
		super(machineName, distribDemonPort, totalNumPages);

		logger = WbRecordReader.getLogger();
		reporter = PigStatusReporter.getInstance();
		if (reporter != null) {
			recordCounter = reporter.getCounter(RecordCounters.PAGES_LOADED);
		}
		else {
			logger.warn("Could not obtain a Web page counter.");
		}

		Socket distributorDemonSocket = Common.getSocket(machineName, 
														 distribDemonPort, 
														 Constants.NUM_DISTRIB_DEMON_ATTEMPTS, 
														 "Get distributor address from distributor demon.");
		
		in	= new DataInputStream(distributorDemonSocket.getInputStream());
		out	= new DataOutputStream(distributorDemonSocket.getOutputStream());
		
		// Ask the distributor demon for a distributor:		
		String distribDemonRequest = "new,0," + 
									 startSite + 
									 "," + 
									 endSite + "," + 
									 System.getProperty("user.name") + "," +
									 Constants.APPLICATION_ID +
									 "\r\n";
		logger.info("Distributor request: " + distribDemonRequest.trim());
		byte[] request = distribDemonRequest.getBytes();
		out.write(request, 0, request.length);
		out.flush();
		
		// Read the answer:
		byte[] distributorIPAndPortArr = new byte[200];
		int responseLen;
		try {
			responseLen = in.read(distributorIPAndPortArr,0, 200);
			if (responseLen < 13)
				throw new IOException("Distributor demon provided unexpected response. Expected <ip><sp><port>. Got: " + new String(distributorIPAndPortArr));
		} catch (Exception e) {
			String errMsg = "Error requesting a distributor from demon at " + machineName + ":" + port + ".\n     " + e.getMessage();
			logger.error(errMsg);
			throw new IOException(errMsg);
		}
		
		out.close();
		in.close();
		distributorDemonSocket.close();
		
		// Build a string from the byte array we read from the socket,
		// and separate the distributor's ip address from the port:
		String[] distributorIPAndPortStrings = new String(distributorIPAndPortArr, 0, responseLen).split("[\\s]");
		if (distributorIPAndPortStrings.length != 2)
			throw new IOException("IP and port returned from distributor demon at " + 
								  machineName + ":" + distribDemonPort + 
								  " has an unexpected format: " + 
								  new String(distributorIPAndPortArr));
		
		// Allow time for the distributor process to start up:
		try {
			Thread.sleep(Constants.TIME_TO_WAIT_FOR_DISTRIBUTOR_TO_START);
		} catch (InterruptedException e) {}
		
		// Open distributor itself:
		Socket distributorSocket = openDistributorSocket(distributorIPAndPortStrings[0], distributorIPAndPortStrings[1]);
		in = new DataInputStream(distributorSocket.getInputStream());
		out = new DataOutputStream(distributorSocket.getOutputStream());
		out.writeInt(totalNumPages);
		
		// Read the one-char doc type of incoming info. It tells us whether
		// the incoming stream is compressed:
		
		logger.info("From " +
				    Constants.getHostInfo() +
				    ": wait for document type byte from distributor... ");
		
		byte docType;
		try {
			// TODO: make sure we don't get stuck here.
			docType = in.readByte();
		} catch (IOException e1) {
			throw new IOException("Could not read from distributor.");
		}
		logger.info("From " +
				    Constants.getHostInfo() +
				    ": received document type byte from distributor. Beginning download... ");
		streamCompressed = (docType & COMPRESSED_MASK) == COMPRESSED_MASK;
		fillBuffer();
	}
	

	/*-----------------------------------------------------
	| openDistributorSocket() 
	------------------------*/

	/**
	 * @param machineName
	 * @param port
	 * @return
	 * @throws IOException
	 */
	private Socket openDistributorSocket(String distributorIP, String distributorPort)
			throws IOException {
		
		Socket distributor = null;
		// Always the pessimist; prepare an error msg:
		String errMsg = "From " +
						Constants.getHostInfo() +
						": could not open a distributor at " + distributorIP + ":" + distributorPort + ". ";
		
		// Set up socket and input/output streams
		logger.info("From " +
				    Constants.getHostInfo() +
				    ": attempt to open socket to distributor at " +
				    distributorIP + ":" + distributorPort);
		try {
			// Allow the distributor demon to start up:
			Thread.sleep(Constants.TIME_TO_WAIT_FOR_DISTRIBUTOR_TO_START);
			// Allow for NUM_DISTRIB_DEMON_ATTEMPTS attempts:
			distributor = Common.getSocket(Common.getInetAddressFromAsciiIP(distributorIP), 
										   Integer.parseInt(distributorPort), 
										   Constants.NUM_DISTRIB_DEMON_ATTEMPTS, 
										   "Opening socket to distributor.");
			} catch (UnknownHostException e) {
				logger.error(errMsg + "Bad IP format: " + distributorIP);
				throw new IOException(errMsg + "Bad IP format: " + distributorIP);
			} catch (NumberFormatException e) {
				logger.error(errMsg + "Bad distributor port: " + distributorPort);
				throw new IOException(errMsg + "Bad distributor port: " + distributorPort);
		} catch (InterruptedException e) {
				logger.error(errMsg + "Interrupted during wait to give time for distributor process to start. " + e.getMessage());
				throw new IOException(errMsg + "Interrupted during wait to give time for distributor process to start." + e.getMessage());
			}
		logger.info("From " +
				    Constants.getHostInfo() +
				    ": success opening socket to distributor at " +
				    distributorIP + ":" + distributorPort);
			
		return distributor;
	}
	
	
	/*-----------------------------------------------------
	| hasNext() 
	------------------------*/
	
	public boolean hasNext() {
		if (wbRecordQueue.isEmpty())
			try {
				fillBuffer();
			} catch (IOException e) {
				return false;
			}
		return !wbRecordQueue.isEmpty();
	}

	/*-----------------------------------------------------
	| next() 
	------------------------*/
	
	public WbRecord next() {
		if (wbRecordQueue.isEmpty())
			try {
				fillBuffer();
				if (wbRecordQueue.isEmpty())
					return null;
			} catch (IOException e) {
				return null;
			}
		return wbRecordQueue.remove();
	}

	/*-----------------------------------------------------
	| remove()
	------------------------*/
	
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	/*-----------------------------------------------------
	| close() 
	------------------------*/
	
	public void close() {
		try {
			in.close();
			out.close();			
		} catch (IOException e) {
			// oh well.
		}
	}
	
	/*-----------------------------------------------------
	| fillBuffer()
	------------------------*/
	
	private int fillBuffer() throws IOException {
		
		int		docID, pageSize, urlLen, tsLen;
		int     truePageLen; // uncompressed len
		long	offset;
		// String page;
		String	timeStamp, url;
		byte[] tsBytes, urlBytes, pageBytes;
		byte[] truePageBytes; // uncompressed
		int numPagesThisRound = 0;
		
		Metadata metadata;
		
		wbRecordQueue.clear();
		for(int i = 0; i < BUFFER_SIZE; i++) {
			try {
				docID			= in.readInt();
				offset			= in.readLong();
				tsLen			= in.readInt();
				urlLen			= in.readInt();
				pageSize		= in.readInt();
						
				if(tsLen > TIMESTAMP_LENGTH || tsLen < 0) {
					String errMsg = "Bad date string length read from page stream.\n    " +
							"Expected max of " + TIMESTAMP_LENGTH + ". Got: " + tsLen +
							" at page# " + totalNumPagesRetrieved + 1;
					logger.warn(errMsg);
					throw new IOException(errMsg);
				}
				if(urlLen > MAX_URL_SIZE || urlLen < 0) {
					String errMsg = "Bad URL string length read from page stream. " +
							"Expected max of " + MAX_URL_SIZE + ". Got: " + urlLen +
							" at page# " + totalNumPagesRetrieved + 1;
					logger.error(errMsg);
					throw new IOException(errMsg);
				}

				if(pageSize > MAX_WEBPAGE_SIZE || pageSize < 0) {
					String errMsg = "Bad page size read from page stream. " +
							"Expected max of " + MAX_WEBPAGE_SIZE + ". Got: " + pageSize + 
							" at page# " + totalNumPagesRetrieved + 1;
					logger.error(errMsg);
					throw new IOException(errMsg);
				}
				
				if(pageSize > MAX_WEBPAGE_SIZE) {
					logger.error("Bad page size read from page stream. " +
							"Expected max of " + MAX_WEBPAGE_SIZE + ". Got: " + pageSize);
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
					
				// Read page itself:
				pageBytes = new byte[pageSize];
				int bytesToRead = pageSize;
				int numRead = 0; 
				while(bytesToRead > 0) {
					numRead = in.read(pageBytes, pageSize - bytesToRead, bytesToRead);
					if(numRead < 1)
						break;
					bytesToRead -= numRead;
				}
				if (streamCompressed) {
					ByteArrayOutputStream uncompressedTxt = decompress(pageBytes, pageSize);
					truePageLen = uncompressedTxt.size();
					truePageBytes = uncompressedTxt.toByteArray();
				} else {
					truePageLen = pageSize;
					truePageBytes = pageBytes;
				}
				// Construct metadata
				metadata = new Metadata(docID, truePageLen, offset, timeStamp, url);
				
				// If the Web page is so defective that the factory
				// refuses to mind a record, then skip it:
				WbRecord wbRecord = WbRecordFactory.getWbRecord(metadata, truePageBytes);
				if (wbRecord == null)
					continue;
				wbRecordQueue.put(wbRecord);
				totalNumPagesRetrieved++;
				numPagesThisRound++;
			} catch(EOFException e) {
				break;
			} catch(IOException e) {
				throw new IOException("Could not read from distributor (note that distributors time out if not read from in a timely fashion): " + e.getMessage());
			} catch (InterruptedException e) {
				throw new IOException("Could not add new page to buffer. Interrupted.");
			}
		}
		if (totalNumPagesRetrieved > nextReporting) {
			String progressMsg = "Total of " + totalNumPagesRetrieved + "Web pages retrieved from " + machineName; 
			logger.info(progressMsg);
			if (reporter != null) {
				reporter.setStatus(progressMsg);
			}
			nextReporting = totalNumPagesRetrieved + Constants.WB_PAGE_LOAD_REPORT_GRANULARITY;
		}
		
		if (recordCounter != null) {
			recordCounter.increment(numPagesThisRound);
		}	
		
		return numPagesThisRound;
	}
	
	private ByteArrayOutputStream decompress(byte[] src, int len) throws IOException{
		final int TMP_BUF_SIZE = 2048;
		byte[] xferBuf = new byte[TMP_BUF_SIZE];
		InflaterInputStream uncompressingIn = null;
		uncompressingIn = new InflaterInputStream(new ByteArrayInputStream(src));
		
		ByteArrayOutputStream clearOut = new ByteArrayOutputStream();
		int numRead;
		try {
			while ((numRead = uncompressingIn.read(xferBuf, 0, TMP_BUF_SIZE)) > 0)
				clearOut.write(xferBuf, 0, numRead);
		} catch (IOException e) {
			String errMsg = "Could not decompress page# " + totalNumPagesRetrieved + 1 + ". " + e.getMessage();
			logger.error(errMsg);
			throw new IOException(errMsg);
		}
		
		return clearOut;
	}
}
