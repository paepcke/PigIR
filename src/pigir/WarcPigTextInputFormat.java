package pigir;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;

public class WarcPigTextInputFormat extends PigTextInputFormat {
  @Override
  public RecordReader<LongWritable, Text>
  			createRecordReader(InputSplit split, 
  							   TaskAttemptContext context) {
    return new WarcRecordReader();
  }
}
