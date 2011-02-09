package pigir;

public class ExtensibleByteArray {

	private final int DEFAULT_SIZE = 500; 
	private byte[] buf = null;
	private int cursor = 0;
	
	/*-----------------------------------------------------
	| Constructors 
	------------------------*/

	public ExtensibleByteArray() {
		buf = new byte[DEFAULT_SIZE];
	}
	
	public ExtensibleByteArray(int initialSize) {
		buf = new byte[initialSize];
	}
	
	/**
	 * Given byte array is copied into a new array. 
	 * Cursor is initialized to just after the initial
	 * contents.
	 * @param initialContents
	 */
	public ExtensibleByteArray(byte[] initialContents) {
		buf = new byte[initialContents.length + DEFAULT_SIZE];
		System.arraycopy(initialContents, 0, buf, 0, initialContents.length);
		cursor = initialContents.length;
	}
	
	/**
	 * Given byte array is copied into a new array. 
	 * Cursor is initialized to given position.
	 * @param initialContents
	 * @param cursorPos
	 */
	
	public ExtensibleByteArray(byte[] initialContents, int cursorPos) {
		buf = new byte[initialContents.length + DEFAULT_SIZE];
		System.arraycopy(initialContents, 0, buf, 0, initialContents.length);
		cursor = cursorPos;
	}
	
	/*-----------------------------------------------------
	| write() 
	------------------------*/

	public void write(byte[] src, int srcOffset, int destCursorPos, int len) {
		cursor = destCursorPos;
		if (len > buf.length - cursor)
			extendBuf();
		System.arraycopy(src, srcOffset, buf, cursor, len);
		cursor += len;
	}
	
	public void write(byte[] src, int len) {
		write(src, 0, cursor, len);
	}
	
	
	/*-----------------------------------------------------
	| extendBuf() 
	------------------------*/
	
	private void extendBuf () {
		byte[] bufTmp = new byte[buf.length + DEFAULT_SIZE];
		System.arraycopy(buf, 0, bufTmp, 0, cursor - 1);
		buf = bufTmp;
		bufTmp = null;
	}
	
}
