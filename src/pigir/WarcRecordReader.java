package pigir;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * Treats keys as offset in file and value as one Warc record. 
 */
public class WarcRecordReader extends RecordReader<LongWritable, Text> {
	
  private static final boolean DO_READ_CONTENT = true;
  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private static final Log LOG = LogFactory.getLog(LineRecordReader.class);
  private long start;
  private long pos;
  private long end;
  private LineAndChunkReader warcLineReader;
  private DataInputStream warcInStream; 
  private LongWritable keyWarcStreamPos = null;
  private WarcRecord valueWarcRecord = null;
  private FSDataInputStream fileIn = null;

    
  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();

    //************
    LOG.info("********* WarcRecordReader initialize called.");
    //************
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(split.getPath());

    try {
    	GZIPInputStream gzWarcInStream = new GZIPInputStream(fileIn,
    													   job.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
    	warcInStream = new DataInputStream(gzWarcInStream);
    } catch (IOException e) {
    	// Not a gzipped file?
    	try {
    		// The attempt to read using the gzip stream above consumed
    		// the first two bytes. Reset:
    		fileIn.seek(0);
    		warcInStream = new DataInputStream (fileIn);
    	} catch (Exception e1) {
    		LOG.info("Could not open WARC split.");
    		return;
    	}
    }
    // TODO: Test slices with non-zero start, and with start > MAX_INTEGER
    boolean skipFirstLine = false;
    if (start != 0) {
    	skipFirstLine = true;
    	--start;
    	// Skipping in a datastream only works for int. So, if
    	// start within the slice is greater than what fits into 
    	// an int, we need to skip in multiple steps:
    	if (start > Integer.MAX_VALUE) {
    		for (int intChunkCount=0; intChunkCount < (start / Integer.MAX_VALUE); intChunkCount++) {
    			warcInStream.skipBytes(Integer.MAX_VALUE);
    		}
    		warcInStream.skipBytes(((Long)(start % (new Long(Integer.MAX_VALUE)))).intValue());
    	}
    	warcInStream.skipBytes((int) (start - pos));
    }
    warcLineReader = new LineAndChunkReader(warcInStream, job);

    if (skipFirstLine) {  // skip first line and re-establish "start".
      start += warcLineReader.readLine(new Text(), 0,
                           (int)Math.min((long)Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
  }
  
  
  public boolean nextKeyValue() throws IOException {
	  return nextKeyValue(DO_READ_CONTENT);
  }
  
  public boolean nextKeyValue(boolean readContents) throws IOException {
    if (keyWarcStreamPos == null) {
      keyWarcStreamPos = new LongWritable();
    }
    keyWarcStreamPos.set(pos);
    valueWarcRecord = WarcRecord.readNextWarcRecord(warcLineReader, readContents);
    if (valueWarcRecord == null) {
    	keyWarcStreamPos = null;
    	return false;
    }
    
    LOG.info("Pulled another WARC record.");
    
    // Update position in the Data stream
    pos += valueWarcRecord.getTotalRecordLength();
    return true;
  }

  @Override
  public LongWritable getCurrentKey() {
    return keyWarcStreamPos;
  }

  @Override
  public WarcRecord getCurrentValue() {
    return valueWarcRecord;
  }

  /**
   * Get the progress within the split
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

