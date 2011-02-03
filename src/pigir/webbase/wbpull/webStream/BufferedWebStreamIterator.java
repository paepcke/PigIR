package pigir.webbase.wbpull.webStream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

import pigir.webbase.Metadata;
import pigir.webbase.WbRecord;
import pigir.webbase.WbRecordFactory;

public class BufferedWebStreamIterator extends WebStream implements Iterator<WbRecord> {

	private static int BUFFER_SIZE = 100;
	private ArrayBlockingQueue<WbRecord> buffer = new ArrayBlockingQueue<WbRecord>(BUFFER_SIZE);
	
	private Socket distributor;
	private	DataInputStream		in;
	private	DataOutputStream	out;
	
	private int totalNumPagesRetrieved = 0;
	
	/*-----------------------------------------------------
	|  Constructors
    ------------------------*/
	
	public BufferedWebStreamIterator(String ip, String port, String startSite, String endSite, int totalNumPages) throws IOException {
		this(ip, Integer.parseInt(port), startSite, endSite, totalNumPages);
	}
	
	public BufferedWebStreamIterator(String ip, int port, String startSite, String endSite, int totalNumPages) throws IOException {
		super(ip, port, totalNumPages);

		// Set up socket and input/output streams
		try {
			distributor = new Socket(ip, port);
		} catch (UnknownHostException e) {
			throw new IOException("Could not open a distributor at " + ip + ":" + port + ". Unknown host.");
		} catch (IOException e) {
			throw new IOException("Could not open a distributor at " + ip + ":" + port + ". " + e.getMessage());
		}
		in	= new DataInputStream(distributor.getInputStream());
		out	= new DataOutputStream(distributor.getOutputStream());
		
		// Request all pages at once:
		out.writeInt(totalNumPages);
		out.writeUTF(startSite);
		out.writeUTF(endSite);
		fillBuffer();
	}

	/*-----------------------------------------------------
	| hasNext() 
	------------------------*/
	
	@Override
	public boolean hasNext() {
		if (buffer.isEmpty())
			try {
				fillBuffer();
			} catch (IOException e) {
				return false;
			}
		return !buffer.isEmpty();
	}

	/*-----------------------------------------------------
	| next() 
	------------------------*/
	
	@Override
	public WbRecord next() {
		if (buffer.isEmpty())
			try {
				fillBuffer();
			} catch (IOException e) {
				return null;
			}
		return buffer.remove();
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
		
		//data received
		@SuppressWarnings("unused") 
		char	incomingDocType; //keep this here because we read it from the connection, even though we don't really use it 
		int		docID, pageSize, urlLen, tsLen;
		long	offset;
		// String page;
		String	timeStamp, url;
		byte[] tsBytes, urlBytes, pageBytes;
		int numPagesThisRound = 0;
		
		Metadata metadata;
		
		buffer.clear();
		//read incoming info
		try {
			incomingDocType	= (new Character((char)in.read())).charValue();
		} catch (IOException e1) {
			throw new IOException("Could not read from distributor.");
		}
			
		for(int i = 0; i < BUFFER_SIZE; i++) {
			try {
				docID			= in.readInt();
				offset			= in.readLong();
				tsLen			= in.readInt();
				urlLen			= in.readInt();
				pageSize		= in.readInt();
						
				if(tsLen > TIMESTAMP_LENGTH || urlLen > MAX_URL_SIZE) { //TODO: check for MAX_FILE_SIZE 
					//TODO: maybe output an error message? just continue for now...
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
					
				//construct metadata
				metadata = new Metadata(docID, offset, timeStamp, url);
								
				//read page
				pageBytes = new byte[pageSize];
				int bytesToRead = pageSize;
				int numRead = 0; 
				while(bytesToRead > 0) {
					numRead = in.read(pageBytes, pageSize - bytesToRead, bytesToRead);
					if(numRead < 1)
						break;
					bytesToRead -= numRead;
				}
				//page = new String(pageBytes, "UTF-8");
				//page = new String(pageBytes, "ISO-8859-1");
				//page = new String(pageBytes, "US-ASCII");
				
				buffer.put(WbRecordFactory.getWbRecord(metadata, pageBytes));
				totalNumPagesRetrieved++;
				numPagesThisRound++;
			} catch(EOFException e) {
				break;
			} catch(IOException e) {
				throw new IOException("Could not read from distributor.");
			} catch (InterruptedException e) {
				throw new IOException("Could not add new page to buffer.");
			}
		}
		return numPagesThisRound;
	}
}
