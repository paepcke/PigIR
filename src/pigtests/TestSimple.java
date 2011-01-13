package pigtests;

import java.util.Properties;

import org.apache.pig.PigServer;


public class TestSimple {

	PigServer pserver;
	Properties props = new Properties();
	
	public TestSimple() {
		System.out.println("Constructor called.");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Enter main of TestSimple.");
		new TestSimple();
		System.out.println("Eexit TestSimple.");
	}

}
