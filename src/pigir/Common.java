package pigir;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public final class Common {
	public static final String PIG_TRUE = "1";
	public static final String PIG_FALSE = "null";
	
	public static void print(PigServer pserver, String token) {
		Iterator<Tuple> res;
		try {
			res = pserver.openIterator(token);
			while (res.hasNext()) {
				System.out.println(res.next());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void print(PigServer pserver, Tuple tuple) {
		for (Object tupleEl : tuple.getAll()) {
			System.out.println(tupleEl);
		}
	}
	
	public static void print(PigServer pserver, Tuple tuple, int limit) {
		for (int i=0; i<limit; i++) {
			try {
				System.out.println(tuple.get(i));
			} catch (ExecException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static File openTmpFile(PigServer pserver) throws IOException {
		
			String tmpDir = pserver.getPigContext().getProperties().getProperty("mapred.child.tmp");
			String userName = System.getProperty("user.name"); 
			if (tmpDir == null || tmpDir.isEmpty())
				tmpDir = "/tmp/" + userName;
			return File.createTempFile(userName, "tmp", new File(tmpDir));
	}
}
