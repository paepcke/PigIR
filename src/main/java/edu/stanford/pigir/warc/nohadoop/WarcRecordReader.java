package edu.stanford.pigir.warc.nohadoop;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.FSDataInputStream;

import edu.stanford.pigir.pigudf.LineAndChunkReader;


/**
 * Treats keys as offset wbRecordReader file and value as one Warc record. 
 */
public class WarcRecordReader {
	
  private static final boolean DO_READ_CONTENT = true;
  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private long start;
  private long pos;
  private long end;
  private LineAndChunkReader warcLineReader;
  private DataInputStream warcInStream; 
  private long keyWarcStreamPos = (long) 0;
  private WarcRecord valueWarcRecord = null;
  private FileInputStream fileIn = null;
  private Logger logger=null;

  public WarcRecordReader(File warcPath) {
	  try {
		  fileIn = new FileInputStream(warcPath);
		  GZIPInputStream gzWarcInStream = new GZIPInputStream(fileIn,DEFAULT_BUFFER_SIZE);
	      warcInStream = new DataInputStream(gzWarcInStream);
	  } catch (IOException e) {
		  // Not a gzipped file?
		  try {
			  // The attempt to read using the gzip stream above consumed
			  // the first two bytes. Reset:
			  fileIn = new FileInputStream(warcPath);
			  warcInStream = new DataInputStream (fileIn);
		  } catch (Exception e1) {
			  logger.info("Could not open WARC file.");
			  return;
		  }
	  }
	  warcLineReader = new LineAndChunkReader(warcInStream);
	  start = 0;
	  pos = 0;
  }
    
  public boolean nextKeyValue() throws IOException {
	  return nextKeyValue(DO_READ_CONTENT);
  }
  
  public boolean nextKeyValue(boolean readContents) throws IOException {
    keyWarcStreamPos = pos;
    valueWarcRecord = WarcRecord.readNextWarcRecord(warcLineReader, readContents);
    if (valueWarcRecord == null) {
    	keyWarcStreamPos = 0;
    	return false;
    }
    
    //System.out.println("Pulled another WARC record.");
    
    // Update position wbRecordReader the Data stream
    pos += valueWarcRecord.getTotalRecordLength();
    return true;
  }

  public long getCurrentKey() {
    return keyWarcStreamPos;
  }

  public WarcRecord getCurrentValue() {
    return valueWarcRecord;
  }

  /**
   * Get the progress within the file:
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException {
    if (warcLineReader	 != null) {
      warcLineReader.close(); 
    }
  }
}

