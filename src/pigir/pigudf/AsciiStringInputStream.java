package pigir.pigudf;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;

public class AsciiStringInputStream extends ObjectInputStream {

	private final int OBJECT_DELIMITER = 0;
	private final int UTF16_CHAR_SIZE  = 2;
    private final int BOOLEAN_SIZE     = 1;
	
	private byte[] source;
	private int pos = 0;
	
	public AsciiStringInputStream(String theSource) throws IOException, SecurityException {
		this(theSource.getBytes());
	}
	
	public AsciiStringInputStream(byte[] theSource) throws IOException, SecurityException {
		super();
		source = theSource;
	}
	
	/* (non-Javadoc)
	 * @see java.io.ObjectInputStream#available()
	 * Return number of characters left to be read.
	 */
	@Override
	public int available() {
		return source.length - pos;
	}
	
	/* (non-Javadoc)
	 * @see java.io.ObjectInputStream#close()
	 * Cannot read from this stream after a call to this method,
	 * or a call to open()
	 */
	@Override
	public void close() {
		pos = -1;
	}
	
	/**
	 * Re-open the stream, resetting to the beginning.
	 * @return True if successful, else false.
	 */
	public boolean open() {
		if (source != null) {
			pos = 0;
			return true;
		}
		else return false;
	}
	
	/* (non-Javadoc)
	 * @see java.io.ObjectInputStream#read()
	 * Read one byte and return it as an integer. The integer
	 * will be an ASCII code between 0 and 127.
	 * <b>Caution</b>: AsciiStringInputStream/AsciiStringOutputStream
	 * use item delimiters in their internal data structures. Using
	 * this method on a stream containing mixtures of integers, floats,
	 * etc., could render subsequent reads of those items incorrect.
	 */
	@Override
	public int read() throws IOException{
		checkEOFIOException();
		return source[pos++]; // read one byte of data
	}
	
	/* (non-Javadoc)
	 * @see java.io.ObjectInputStream#read(byte[], int, int)
	 * Read len bytes into the passed-in buf. Stream will be
	 * advanced. 
	 * <b>Caution</b>: AsciiStringInputStream/AsciiStringOutputStream
	 * use item delimiters in their internal data structures. Using
	 * this method on a stream containing mixtures of integers, floats,
	 * etc., could render subsequent reads of those items incorrect.
	 */
	@Override
	public int read(byte[] buf, int off, int len) throws IOException {
		checkEOFIOException();
		if (pos < 0)
			return -1;
		int numRead = Math.min(len, source.length - pos);
		for (int i=off; i < off + numRead - 1; i++)
			buf[i] = source[pos++];
		return numRead;
	}
	
	/* (non-Javadoc)
	 * @see java.io.ObjectInputStream#readBoolean()
	 * Read from the stream information that was written to 
	 * the stream via AsciiStringOutputStream's writeBoolean().
	 */
	@Override
	public boolean readBoolean() throws EOFException {
		checkEOF(BOOLEAN_SIZE);  // we use one byte: "1" or "0"
		byte theBool = source[pos++];
		pos++;  // skip the null terminator
		if ((char) theBool == '1')
			return true;
		else
			return false;
	}
	
	/* (non-Javadoc)
	 * @see java.io.ObjectInputStream#readByte()
	 * Return one byte from the stream, advancing the cursor.
	 * <b>Caution</b>: AsciiStringInputStream/AsciiStringOutputStream
	 * use item delimiters in their internal data structures. Using
	 * this method on a stream containing mixtures of integers, floats,
	 * etc., could render subsequent reads of those items incorrect.
	 */
	@Override
	public byte readByte() throws EOFException {
		checkEOF();
		byte res = source[pos++];
		pos++;
		return res;
	}
	
	/* (non-Javadoc)
	 * @see java.io.ObjectInputStream#readChar()
	 * Return a 16-bit char.
	 * <b>Caution</b>: AsciiStringInputStream/AsciiStringOutputStream
	 * use item delimiters in their internal data structures. Using
	 * this method on a stream containing mixtures of integers, floats,
	 * etc., could render subsequent reads of those items incorrect.
	 */
	@Override
	public char readChar() throws EOFException  {
		checkEOF(UTF16_CHAR_SIZE);
		int res = (int) source[pos];
		pos += UTF16_CHAR_SIZE + 1; // 16 bit UTF plus delimiter
		return (char) res;
	}
	
	/* (non-Javadoc)
	 * @see java.io.ObjectInputStream#readDouble()
	 */
	@Override
	public double readDouble() throws EOFException, NumberFormatException {
		byte[] buf = getArraySlice();
		double res = Double.parseDouble(new String(buf));
		return res;
	}

	/* (non-Javadoc)
	 * @see java.io.ObjectInputStream#readFloat()
	 */
	@Override
	public float readFloat() throws EOFException, NumberFormatException {
		byte[] buf = getArraySlice();
		float res = Float.parseFloat(new String(buf));
		return res;
	}
	
	/* (non-Javadoc)
	 * @see java.io.ObjectInputStream#readFully(byte[])
	 * Copy the remainder of the stream to the provided
	 * buf. 
	 * Note that if that remainder includes items like
	 * integers, or floats, then those items will have been 
	 * delimited from each other by a control sequence. This
	 * sequence will be copied along with the payload. Use this
	 * method only when you know the nature of the stream
	 * content. 
	 */
	@Override
	public void readFully(byte[] buf) throws EOFException {
		checkEOF();
		int j = 0;
		for (int i=pos; i<source.length; i++)
			buf[j++] = source[i];
		pos = -1;
	}
	
	/* (non-Javadoc)
	 * @see java.io.ObjectInputStream#readFully(byte[], int, int)
	 * Note that if that remainder includes items like
	 * integers, or floats, then those items will have been 
	 * delimited from each other by a control sequence. This
	 * sequence will be copied along with the payload. Use this
	 * method only when you know the nature of the stream
	 * content. 
	 */
	@Override
	public void readFully(byte[] buf, int off, int len) throws EOFException {
		checkEOF(len);
		int j = off;
		for (int i=pos; i<pos+len; i++)
			buf[j++] = source[i];  
		pos += len;
	}
	
	@Override
	public int readInt() throws EOFException, NumberFormatException {
		byte[] buf = getArraySlice();
		int res = Integer.parseInt(new String(buf));
		return res;
	}
	
	@Override
	public long readLong() throws EOFException, NumberFormatException {
		byte[] buf = getArraySlice();
		long res = Long.parseLong(new String(buf));
		return res;
	}
	
	@Override
	public short readShort() throws EOFException, NumberFormatException {
		byte[] buf = getArraySlice();
		short res = Short.parseShort(new String(buf));
		return res;
	}
	
	@Override
	public int readUnsignedByte() throws EOFException { 
		return (int) readInt();
	}

	@Override
	public int readUnsignedShort() throws EOFException { 
		return (int) readByte();
	}

	@Override
	public String readUTF() throws EOFException {
		byte[] buf = getArraySlice();
		return new String(buf);
	}
	
	@Override
	public int skipBytes(int len) {
		int lenSkipped = Math.min(len, source.length - pos);
		pos += len;
		if (pos >= source.length)
			pos = -1;
		return lenSkipped;
	}
	
	private byte[] getArraySlice() throws EOFException {
		for (int i=pos; i<source.length; i++) {
			if (source[i] == OBJECT_DELIMITER) {
				int sliceLen = i - pos;
				byte[] res = new byte[sliceLen];
				// Copy substring to pos 0 in our result array:
				System.arraycopy(source, pos, res, 0, sliceLen);
				pos = i + 1; // pt past the delimiter
				if (pos == source.length)
					pos = -1;
				return res;
			}
		}
		// Didn't find str delimiter before end of the slice.
		pos = -1;
		throw new EOFException("Attempt to read past end of string while reading an object.");
	}
	
	private void checkEOFIOException() throws IOException {
		if (pos < 0)
			throw new IOException("Attempt to read from stream that has reached its end.");
	}
	
	private void checkEOF () throws EOFException {
		if (pos < 0)
			throw new EOFException("Attempt to read from stream that has reached its end.");
	}
	
	private void checkEOF(int numBytesToRead) throws EOFException {
		if (numBytesToRead > source.length - pos)
			throw new EOFException("Reached end of string in while trying to read " + numBytesToRead + " bytes.");
	}
}
