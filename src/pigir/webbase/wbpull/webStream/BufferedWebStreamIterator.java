package pigir.webbase.wbpull.webStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.InflaterInputStream;

import org.apache.log4j.Logger;

import pigir.webbase.Constants;
import pigir.webbase.Metadata;
import pigir.webbase.WbRecord;
import pigir.webbase.WbRecordFactory;
import pigir.webbase.WbRecordReader;

public class BufferedWebStreamIterator extends WebStream implements Iterator<WbRecord> {

	private static int BUFFER_SIZE = 100;
	
	private static byte COMPRESSED_MASK = 2;
	private ArrayBlockingQueue<WbRecord> wbRecordQueue = new ArrayBlockingQueue<WbRecord>(BUFFER_SIZE);
	
	private	DataInputStream		in;
	private	DataOutputStream	out;
	
	private int nextReporting = Constants.WB_PAGE_LOAD_REPORT_GRANULARITY;
	
	private Logger logger = null;
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

		Socket distributorDemonSocket = openDistribDemonSocket(machineName, distribDemonPort);
		String[] distribIPAndPort = getDistributorIPAndPort(machineName, distribDemonPort, distributorDemonSocket);
		String distributorIP   = distribIPAndPort[0];
		String distributorPort = distribIPAndPort[1];
		
		Socket distributorSocket = openDistributorSocket(distributorIP, distributorPort);
		in	= new DataInputStream(distributorSocket.getInputStream());
		out	= new DataOutputStream(distributorSocket.getOutputStream());

		// Request all pages at once:
		String request = "" + totalNumPages + "," + startSite + "," + endSite + "\n";
		out.write(request.getBytes());
		
		// Read the one-char doc type of incoming info. It tells us whether
		// the incoming stream is compressed:
		byte docType;
		try {
			docType = in.readByte();
		} catch (IOException e1) {
			throw new IOException("Could not read from distributor.");
		}
		streamCompressed = (docType & COMPRESSED_MASK) == COMPRESSED_MASK;
		fillBuffer();
	}
	
	/*-----------------------------------------------------
	| openDistribDemonSocket() 
	------------------------*/

	/**
	 * @param machineName
	 * @param port
	 * @return
	 * @throws IOException
	 */
	private Socket openDistribDemonSocket(String machineName, int port)
			throws IOException {
		Socket distributor;		
		// Set up socket and input/output streams
		try {
			distributor = new Socket(machineName, port);
		} catch (UnknownHostException e) {
			String errMsg = "Could not open a distributor at " + machineName + ":" + port + ". Unknown host.";
			logger.error(errMsg);
			throw new IOException(errMsg);
		} catch (IOException e) {
			String errMsg = "Could not open a distributor at " + machineName + ":" + port + ". " + e.getMessage();
			logger.error(errMsg);
			throw new IOException(errMsg);
		}
		return distributor;
	}

	/*-----------------------------------------------------
	| getDistributorIPAndPort()
	------------------------*/

	/**
	 * @param machineName
	 * @param port
	 * @param distributorDemonSocket
	 * @throws IOException
	 */
	private String[] getDistributorIPAndPort(String machineName, int port,
			Socket distributorDemonSocket) throws IOException {
		
		int responseLen = -1;
		
		in	= new DataInputStream(distributorDemonSocket.getInputStream());
		out	= new DataOutputStream(distributorDemonSocket.getOutputStream());
		
		logger.info("Obtained distributor demon at " + machineName + ":" + port + ". Requesting distributor.");
		
		out.writeBytes("new,0,\r\n");
		out.flush();
		
		// Ask the distributor demon for a distributor:
		byte[] distributorIPAndPortArr = new byte[200];
		try {
			responseLen = in.read(distributorIPAndPortArr,0, 200);
			if (responseLen < 13)
				throw new IOException("Distributor demon provided unexpected response. Expected <ip><sp><port>. Got: " + new String(distributorIPAndPortArr));
		} catch (Exception e) {
			String errMsg = "Error requesting a distributor from demon at " + machineName + ":" + port + ".\n     " + e.getMessage();
			logger.error(errMsg);
			throw new IOException(errMsg);
		}
		in.close();
		out.close();
		distributorDemonSocket.close();

		String response = new String(distributorIPAndPortArr, 0, responseLen); 
		return response.split(" ");
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
		
		Socket distributor;
		// Always the pessimist:
		String errMsg = "Could not open a distributor at " + distributorIP + ":" + distributorPort + ". ";
		
		// Set up socket and input/output streams
		try {
			distributor = new Socket(getInetAddressFromAsciiIP(distributorIP), Integer.parseInt(distributorPort));
		} catch (UnknownHostException e) {
			logger.error(errMsg + "Bad IP format: " + distributorIP);
			throw new IOException(errMsg + "Bad IP format: " + distributorIP);
		} catch (NumberFormatException e) {
			logger.error(errMsg + "Bad distributor port: " + distributorPort);
			throw new IOException(errMsg + "Bad distributor port: " + distributorPort);
		} catch (IOException e) {
			logger.error(errMsg + e.getMessage());
			throw new IOException(errMsg + e.getMessage());
		}
		return distributor;
	}
	
	private InetAddress getInetAddressFromAsciiIP(String ip) throws UnknownHostException {
		String[] ipComponents = ip.split("\\.");
		byte[] ipBytes = {(byte) Integer.parseInt(ipComponents[0]), 
				          (byte) Integer.parseInt(ipComponents[1]),
				          (byte) Integer.parseInt(ipComponents[2]),
				          (byte) Integer.parseInt(ipComponents[3])
				                                          
				};
		return InetAddress.getByAddress(ipBytes);
	}
	
	/*-----------------------------------------------------
	| hasNext() 
	------------------------*/
	
	@Override
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
	
	@Override
	public WbRecord next() {
		if (wbRecordQueue.isEmpty())
			try {
				fillBuffer();
			} catch (IOException e) {
				return null;
			}
		return wbRecordQueue.remove();
	}

	/*-----------------------------------------------------
	| remove()
	------------------------*/
	
	@Override
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
				
				wbRecordQueue.put(WbRecordFactory.getWbRecord(metadata, truePageBytes));
				totalNumPagesRetrieved++;
				numPagesThisRound++;
			} catch(EOFException e) {
				break;
			} catch(IOException e) {
				throw new IOException("Could not read from distributor: " + e.getMessage());
			} catch (InterruptedException e) {
				throw new IOException("Could not add new page to buffer. Interrupted.");
			}
		}
		if (totalNumPagesRetrieved > nextReporting) {
			logger.info("Total of " + totalNumPagesRetrieved + "Web pages retrieved from " + machineName);
			nextReporting = totalNumPagesRetrieved + Constants.WB_PAGE_LOAD_REPORT_GRANULARITY;
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
