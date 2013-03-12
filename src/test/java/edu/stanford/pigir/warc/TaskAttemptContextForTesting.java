package edu.stanford.pigir.warc;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

public class TaskAttemptContextForTesting implements TaskAttemptContext {

	public TaskAttemptContextForTesting(Configuration conf, TaskAttemptID taskId) {
		super();
	}

	public TaskAttemptContextForTesting() {
		this(null, null);
	}	
	
	@Override
	public Configuration getConfiguration() {
		// TODO Auto-generated method stub
		return new Configuration();
	}

	@Override
	public Credentials getCredentials() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JobID getJobID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getNumReduceTasks() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Path getWorkingDirectory() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<?> getOutputKeyClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<?> getOutputValueClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<?> getMapOutputKeyClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<?> getMapOutputValueClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getJobName() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean userClassesTakesPrecedence() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Class<? extends InputFormat<?, ?>> getInputFormatClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class<? extends Partitioner<?, ?>> getPartitionerClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RawComparator<?> getSortComparator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getJar() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RawComparator<?> getGroupingComparator() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean getJobSetupCleanupNeeded() {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean getProfileEnabled() {
		// TODO Auto-generated method stub
		return false;
	}

	public String getProfileParams() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getUser() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean getSymlink() {
		// TODO Auto-generated method stub
		return false;
	}

	public Path[] getArchiveClassPaths() {
		// TODO Auto-generated method stub
		return null;
	}

	public URI[] getCacheArchives() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public URI[] getCacheFiles() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Path[] getLocalCacheArchives() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Path[] getLocalCacheFiles() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Path[] getFileClassPaths() {
		// TODO Auto-generated method stub
		return null;
	}

	public String[] getArchiveTimestamps() {
		// TODO Auto-generated method stub
		return null;
	}

	public String[] getFileTimestamps() {
		// TODO Auto-generated method stub
		return null;
	}

	public int getMaxMapAttempts() {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getMaxReduceAttempts() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void progress() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public TaskAttemptID getTaskAttemptID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setStatus(String msg) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

}
