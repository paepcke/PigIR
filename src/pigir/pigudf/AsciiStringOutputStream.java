package pigir.pigudf;

import java.io.IOException;
import java.io.ObjectOutputStream;

public class AsciiStringOutputStream extends ObjectOutputStream {
	
	private final int OBJECT_DELIMITER = 0;
	String dest = "";

	public AsciiStringOutputStream() throws IOException, SecurityException {
		super();
	}

	@Override
	public void reset() {
		dest = "";
	}

	@Override
	public void write(byte[] buf) throws IOException {
		ensureAscii(buf, 0, buf.length);
		dest += new String(buf);
		addDelimiter();
	}
	
	@Override
	public void write(byte[] buf, int off, int len) throws IOException {
		byte[] slice = new byte[len];
		byte oneByte;
		for (int i=off; i<=off+len-1; i++) { 
			oneByte = buf[i];
			if (!isAscii(oneByte))
				throw new IOException("StringOutputStream is only intended for US_ASCII. Attempt to write int " +
						new Byte(oneByte).toString());
			slice[i] = buf[i];
		}
		dest += new String(slice);
		addDelimiter();
	}
	
	@Override
	public void write(int val) throws IOException {
		if (!isAscii(val))
			throw new IOException("StringOutputStream is only intended for US_ASCII. Attempt to write int " + val);
		dest += (char)val;
		addDelimiter();
	}

	@Override
	public void writeBoolean(boolean val) {
		if (val)
			dest += "1";
		else
			dest += "0";
		addDelimiter();
	}
	
	@Override
	public void writeByte(int val) throws IOException {
		write(val);
		addDelimiter();
	}
	
	@Override
	public void writeBytes (String val) {
		dest += val;
		addDelimiter();
	}
	
	@Override
	public void writeChar(int val) throws IOException {
		write(val);
		addDelimiter();
	}
	
	@Override
	public void writeChars(String val) {
		dest += val;
		addDelimiter();
	}
	
	@Override
	public void writeDouble(double val) {
		dest += new Double(val).toString();
		addDelimiter();
	}
	
	@Override
	public void writeFloat(float val) {
		dest += new Float(val).toString();
		addDelimiter();
	}
	
	@Override
	public void writeInt(int val) {
		dest += new Integer(val).toString();
		addDelimiter();
	}
	
	@Override
	public void writeLong(long val) {
		dest += new Long(val).toString();
		addDelimiter();
	}
	
	@Override
	public void writeShort(int val) throws IOException {
		write(val);
	}
	
	@Override
	public void writeUTF(String val) throws IOException {
		write(val.getBytes());
	}
	
	@Override
	public void close() {
	}
	
	@Override
	public void flush() {
	}
	
	@Override
	public String toString() {
		return dest;
	}
	
	private boolean isAscii(byte b) {
		return ((b > 0) && (b < 127));
	}
	
	private boolean isAscii(int b) {
		return ((b > 0) && (b < 127));
	}
	
	private void ensureAscii(byte[] buf, int off, int len) throws IOException {
		byte theByte;
		for (int i=off; i<=off+len-1; i++) {
			theByte = buf[i];
			if (!isAscii(theByte))
				throw new IOException("StringOutputStream is only intended for US_ASCII. Attempt to write int " +
						new Byte(theByte).toString());
		}
	}
	
	private void addDelimiter() {
		dest += (char) OBJECT_DELIMITER;
	}
}
