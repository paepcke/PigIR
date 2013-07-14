package edu.stanford.pigir.irclientserver;

import com.esotericsoftware.minlog.Log.Logger;

public class ConsoleLogger extends Logger {
	
	public void log (int level, String category, String message, Throwable ex) {
		System.out.println(message);
	}
	
}

