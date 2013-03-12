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

// Cloudera Hadoop has TaskAttemptContext be an Interface,
// while Apache had it as a class:
public class TaskAttemptContextForTesting extends TaskAttemptContext {

	public TaskAttemptContextForTesting(Configuration conf, TaskAttemptID taskId) {
		super();
	}

	public TaskAttemptContextForTesting() {
		this(null, null);
	}	
	
	public Configuration getConfiguration() {
		// TODO Auto-generated method stub
		return new Configuration();
	}

	public Credentials getCredentials() {
		// TODO Auto-generated method stub
		return null;
	}

	public JobID getJobID() {
		// TODO Auto-generated method stub
		return null;
	}

	public int getNumReduceTasks() {
		// TODO Auto-generated method stub
		return 0;
	}

	public Path getWorkingDirectory() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Class<?> getOutputKeyClass() {
		// TODO Auto-generated method stub
		return null;
	}

	public Class<?> getOutputValueClass() {
		// TODO Auto-generated method stub
		return null;
	}

	public Class<?> getMapOutputKeyClass() {
		// TODO Auto-generated method stub
		return null;
	}

	public Class<?> getMapOutputValueClass() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getJobName() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean userClassesTakesPrecedence() {
		// TODO Auto-generated method stub
		return false;
	}

	public Class<? extends InputFormat<?, ?>> getInputFormatClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	public Class<? extends Partitioner<?, ?>> getPartitionerClass()
			throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

	public RawComparator<?> getSortComparator() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getJar() {
		// TODO Auto-generated method stub
		return null;
	}

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

	public void progress() {
		// TODO Auto-generated method stub
		
	}

	public TaskAttemptID getTaskAttemptID() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setStatus(String msg) {
		// TODO Auto-generated method stub
		
	}

	public String getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

}
