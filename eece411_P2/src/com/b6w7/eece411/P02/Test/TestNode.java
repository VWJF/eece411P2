package com.b6w7.eece411.P02.Test;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.b6w7.eece411.P02.multithreaded.JoinThread;
//import com.b6w7.eece411.P02.Node;
import com.b6w7.eece411.P02.multithreaded.NodeCommands;
import com.b6w7.eece411.P02.multithreaded.Service;
/**
 * A test class for testing {@link Service}
 * 
 * @author Scott Hazlett
 * @author Ishan Sahay
 * @date 23 Jan 2014
 */
public class TestNode implements Runnable, JoinThread {

	private static final long TIME_RETRY_MS = 2000;

	private static final int NUM_THREADS_IN_POOL = 40;

	private static int NUM_TEST_RUNNABLES = 10;  // total placed keys = 100 * 400 = 40000 

	//private static int count = 0; 
	private int myCount;

	// set to 0 to disable timeout
	private final int TCP_READ_TIMEOUT_MS = 0;
	// extra debug output from normal
	private static boolean IS_VERBOSE = false;
	// reduced debug outut from normal
	private static boolean IS_BREVITY = false;

	private MessageDigest md;

	// counters for total number of individual tests that passed/failed
	private static AtomicInteger testPassed = new AtomicInteger(0);
	private static AtomicInteger testFailed = new AtomicInteger(0);
	// counter for non-completed number of threads running.  Used to shutdown thread pool when == 0
	private static AtomicInteger numTestsRunning = new AtomicInteger(NUM_TEST_RUNNABLES);

	// list of test cases, each entry representing one test case
	private List<TestData> tests = new LinkedList<TestData>();

	private final String url;
	private final int port;

	private static Set<Integer> intList = new HashSet<Integer>(NUM_TEST_RUNNABLES * 100);

	// we are listening, so now allocated a ThreadPool to handle new sockets connections
	private static ExecutorService executor;

	private static void printUsage() {
		System.out.println("USAGE:\n"
				+ "  java -cp"
				+ " <file.jar>"
				+ " " + TestNode.class.getCanonicalName() 
				+ " <server URL>"  
				+ " <server port>");
		System.out.println("EXAMPLE:\n"
				+ "  java -cp"
				+ " A1.jar"
				+ " " + TestNode.class.getCanonicalName() 
				+ " reala.ece.ubc.ca"  
				+ " 5627");
	}


	private void populateOneTest(byte cmd, String keyString, String valueString, byte reply) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		// we can reuse 'value' for both the sending of a PUT as well as receiving of a GET operation,
		// so no need two arguments

		// If we want to repeated append to hash before digesting, call update() 
		//			md.update("Scott".getBytes(StandardCharsets.UTF_8.displayName()));
		//			key.put(md.digest()); 

		if (null == md) 
			md = MessageDigest.getInstance("SHA-1");

		ByteBuffer hashedKey = null;
		ByteBuffer value = null;

		// Massage parameters into a TestData object that is appended to 'tests'
		// which will then be iterated through in the test harness

		// Note that we create hash of key which will be padded with zeroes at end.
		// Also, if hashing algorithm changes (SHA-1 creates a 20 bytes hash) which 
		// becomes larger than our hash limit (32b) then the logic exists here to
		// handle that case (it will be truncated)
		try {
			hashedKey = ByteBuffer.allocate(NodeCommands.LEN_KEY_BYTES);
			value = ByteBuffer.allocate(NodeCommands.LEN_VALUE_BYTES);

			// create hash
			// if we want to increase entropy in the hash, this would be the line to do it
			// byte[] digest = md.digest(keyString.getBytes(StandardCharsets.UTF_8.displayName()));
			byte[] digest = md.digest(keyString.getBytes("UTF-8"));

			if (digest.length > hashedKey.limit()) {
				hashedKey.put( digest, 0, hashedKey.limit() );
			} else {
				hashedKey.put( digest );
			}
			// value.put(valueString.getBytes(StandardCharsets.UTF_8.displayName()));
			value.put(valueString.getBytes("UTF-8"));
			//reply = NodeCommands.Reply.RPY_SUCCESS.getCode();

			if (NodeCommands.Request.CMD_PUT.getCode() == cmd) {
				// If we are performing a PUT, then we need to send value
				tests.add(new TestData(cmd, hashedKey, value, reply, null));

			} else if (NodeCommands.Request.CMD_GET.getCode() == cmd){
				// If we are performing a GET, then we do not have to send 'value', 
				// but we must see the value replied to us
				tests.add(new TestData(cmd, hashedKey, null, reply, value));

			} else if (NodeCommands.Request.CMD_REMOVE.getCode() == cmd){
				// If we are performing a REMOVE, then we do not have to send 'value',
				// and neither do we have to expect it as a returned value
				tests.add(new TestData(cmd, hashedKey, null, reply, null));

			} else {
				// Unrecognized Commands, the server should handle this case gracefully
				tests.add(new TestData(cmd, hashedKey, value, reply, null));
				//throw new IllegalArgumentException("Unknown command");
			}

		} catch (BufferOverflowException e) {
			System.err.println("test skipped for "+keyString+"=>"+valueString+"\nvalue exceeds "+NodeCommands.LEN_VALUE_BYTES+" bytes");
			System.out.println("Thread: "+myCount+"\ntest skipped for "+keyString+"=>"+valueString+"\nvalue exceeds "+NodeCommands.LEN_VALUE_BYTES+" bytes");
		}
	}

	private void populateOneTest() throws NoSuchAlgorithmException, UnsupportedEncodingException {
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"1Scott", "a63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
//		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"2Scott", "b63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
//		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"3Scott", "c63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
//		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"4Scott", "d63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
//		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"5Scott", "e63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
//		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"6Scott", "f63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"1Scott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
	}
	private void populateTests() throws NoSuchAlgorithmException, UnsupportedEncodingException {
		// test 1: put 'Scott' => '63215065', and so on ... 

		// Create a random number and ensure it is unique across all tests
		myCount = new Random().nextInt(Integer.MAX_VALUE);
		synchronized (intList) {
			while (intList.contains(myCount))
				myCount = new Random().nextInt(Integer.MAX_VALUE);
			intList.add(myCount);
		}
		
		// System.out.println(myCount);	
		//		String myCount = Thread.currentThread().toString(); //String does not change with different threads
		//		myCount = Integer.toString(new Random().nextInt(NUM_TEST_THREADS*NUM_TEST_THREADS));		

		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"Scott", "63215065", NodeCommands.Reply.RPY_INEXISTENT.getCode());
		populateOneTest(NodeCommands.Request.CMD_REMOVE.getCode(), myCount+"Scott", "63215065", NodeCommands.Reply.RPY_INEXISTENT.getCode());

		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"Scott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"Ishan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"ssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());

		//		populateOneTest(NodeCommands.CMD_PUT, myCount+"John", "Smith", NodeCommands.RPY_OUT_OF_SPACE);

		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"Scott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"Ishan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"ssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());

		populateOneTest(NodeCommands.Request.CMD_REMOVE.getCode(), myCount+"Scott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_REMOVE.getCode(), myCount+"Ishan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_REMOVE.getCode(), myCount+"ssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());

		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"Scott", "63215065", NodeCommands.Reply.RPY_INEXISTENT.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"Ishan", "Sahay", NodeCommands.Reply.RPY_INEXISTENT.getCode());

		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"localhost", "137.82.52.29", NodeCommands.Reply.RPY_INEXISTENT.getCode());

		populateOneTest(NodeCommands.Request.CMD_UNRECOG.getCode(), myCount+"Fake", "Fake", NodeCommands.Reply.RPY_UNRECOGNIZED.getCode());

		populateOneTest((byte)0xFF, myCount+"OutOfBounds0", "OutOfBounds", NodeCommands.Reply.RPY_UNRECOGNIZED.getCode());
		populateOneTest((byte)0x7F, myCount+"OutOfBounds4", "OutOfBounds", NodeCommands.Reply.RPY_UNRECOGNIZED.getCode());
		populateOneTest((byte)0x80, myCount+"OutOfBounds1", "OutOfBounds", NodeCommands.Reply.RPY_UNRECOGNIZED.getCode());
		populateOneTest((byte)0x88, myCount+"OutOfBounds2", "OutOfBounds", NodeCommands.Reply.RPY_UNRECOGNIZED.getCode());
		populateOneTest((byte)0xC0, myCount+"OutOfBounds3", "OutOfBounds", NodeCommands.Reply.RPY_UNRECOGNIZED.getCode());
		populateOneTest((byte)0xEE, myCount+"OutOfBounds4", "OutOfBounds", NodeCommands.Reply.RPY_UNRECOGNIZED.getCode());

	}

	// puts 100 keys, and then gets the same 100 keys
	private void populateMemoryTests() throws NoSuchAlgorithmException, UnsupportedEncodingException {
		// test 1: put 'Scott' => '63215065', and so on ... 

		myCount = new Random().nextInt(Integer.MAX_VALUE);		
		System.out.println(myCount);

		//		populateOneTest(NodeCommands.CMD_GET, myCount+"Scott", "63215065", NodeCommands.RPY_INEXISTENT);
		//		populateOneTest(NodeCommands.CMD_REMOVE, myCount+"Scott", "63215065", NodeCommands.RPY_INEXISTENT);

		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAScott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAIshan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAHazlett", "Hazlett", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAMarco", "Polo", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAOld", "MacDonald", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAreala.ece.ubc.ca", "QQorRQ", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAfinance hub of the world", "Abu Dhabi", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAGotta get up 1234", "Get up and rooool 5678", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAeee iii eee ii ooo", "sheep", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAbombastic", "is the word??", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAgettysburg address", "Where exactly?", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAADrumming up sounds", "In the drumtown land", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAtypical qwerty", "with lousy shift key", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAnot@home.com", "telling you now", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"AAAi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());

		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBScott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBIshan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBHazlett", "Hazlett", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBMarco", "Polo", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBOld", "MacDonald", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBreala.ece.ubc.ca", "QQorRQ", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBfinance hub of the world", "Abu Dhabi", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBGotta get up 1234", "Get up and rooool 5678", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBeee iii eee ii ooo", "sheep", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBbombastic", "is the word??", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBgettysburg address", "Where exactly?", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBDrumming up sounds", "In the drumtown land", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBtypical qwerty", "with lousy shift key", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBnot@home.com", "telling you now", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"BBBi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());

		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCScott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCIshan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCHazlett", "Hazlett", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCMarco", "Polo", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCOld", "MacDonald", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCreala.ece.ubc.ca", "QQorRQ", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCfinance hub of the world", "Abu Dhabi", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCGotta get up 1234", "Get up and rooool 5678", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCeee iii eee ii ooo", "sheep", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCbombastic", "is the word??", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCgettysburg address", "Where exactly?", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCDrumming up sounds", "In the drumtown land", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCtypical qwerty", "with lousy shift key", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCnot@home.com", "telling you now", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"CCCi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());

		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDScott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDIshan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDHazlett", "Hazlett", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDMarco", "Polo", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDOld", "MacDonald", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDreala.ece.ubc.ca", "QQorRQ", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDfinance hub of the world", "Abu Dhabi", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDGotta get up 1234", "Get up and rooool 5678", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDeee iii eee ii ooo", "sheep", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDbombastic", "is the word??", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDgettysburg address", "Where exactly?", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDDrumming up sounds", "In the drumtown land", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDtypical qwerty", "with lousy shift key", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDnot@home.com", "telling you now", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"DDDi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());

		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEScott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEIshan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEHazlett", "Hazlett", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEMarco", "Polo", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEOld", "MacDonald", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEreala.ece.ubc.ca", "QQorRQ", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEfinance hub of the world", "Abu Dhabi", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEGotta get up 1234", "Get up and rooool 5678", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEeee iii eee ii ooo", "sheep", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEbombastic", "is the word??", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEgettysburg address", "Where exactly?", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEDrumming up sounds", "In the drumtown land", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEtypical qwerty", "with lousy shift key", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEnot@home.com", "telling you now", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"EEEi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());

		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFScott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFIshan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFHazlett", "Hazlett", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFMarco", "Polo", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFOld", "MacDonald", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFreala.ece.ubc.ca", "QQorRQ", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFfinance hub of the world", "Abu Dhabi", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFGotta get up 1234", "Get up and rooool 5678", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFFFF iii FFF ii ooo", "sheep", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFbombastic", "is the word??", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFgettysburg address", "Where exactly?", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFDrumming up sounds", "In the drumtown land", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFtypical qwerty", "with lousy shift key", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFnot@home.com", "telling you now", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"FFFi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());

		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"GGGi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"HHHi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"IIIi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_PUT.getCode(), myCount+"JJJi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());

		
	
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAScott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAIshan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAHazlett", "Hazlett", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAMarco", "Polo", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAOld", "MacDonald", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAreala.ece.ubc.ca", "QQorRQ", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAfinance hub of the world", "Abu Dhabi", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAGotta get up 1234", "Get up and rooool 5678", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAeee iii eee ii ooo", "sheep", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAbombastic", "is the word??", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAgettysburg address", "Where exactly?", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAADrumming up sounds", "In the drumtown land", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAtypical qwerty", "with lousy shift key", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAnot@home.com", "telling you now", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"AAAi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());

		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBScott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBIshan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBHazlett", "Hazlett", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBMarco", "Polo", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBOld", "MacDonald", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBreala.ece.ubc.ca", "QQorRQ", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBfinance hub of the world", "Abu Dhabi", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBGotta get up 1234", "Get up and rooool 5678", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBeee iii eee ii ooo", "sheep", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBbombastic", "is the word??", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBgettysburg address", "Where exactly?", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBDrumming up sounds", "In the drumtown land", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBtypical qwerty", "with lousy shift key", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBnot@home.com", "telling you now", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"BBBi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());

		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCScott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCIshan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCHazlett", "Hazlett", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCMarco", "Polo", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCOld", "MacDonald", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCreala.ece.ubc.ca", "QQorRQ", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCfinance hub of the world", "Abu Dhabi", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCGotta get up 1234", "Get up and rooool 5678", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCeee iii eee ii ooo", "sheep", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCbombastic", "is the word??", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCgettysburg address", "Where exactly?", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCDrumming up sounds", "In the drumtown land", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCtypical qwerty", "with lousy shift key", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCnot@home.com", "telling you now", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"CCCi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());

		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDScott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDIshan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDHazlett", "Hazlett", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDMarco", "Polo", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDOld", "MacDonald", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDreala.ece.ubc.ca", "QQorRQ", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDfinance hub of the world", "Abu Dhabi", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDGotta get up 1234", "Get up and rooool 5678", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDeee iii eee ii ooo", "sheep", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDbombastic", "is the word??", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDgettysburg address", "Where exactly?", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDDrumming up sounds", "In the drumtown land", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDtypical qwerty", "with lousy shift key", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDnot@home.com", "telling you now", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"DDDi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());
	
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEScott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEIshan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEHazlett", "Hazlett", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEMarco", "Polo", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEOld", "MacDonald", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEreala.ece.ubc.ca", "QQorRQ", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEfinance hub of the world", "Abu Dhabi", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEGotta get up 1234", "Get up and rooool 5678", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEeee iii eee ii ooo", "sheep", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEbombastic", "is the word??", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEgettysburg address", "Where exactly?", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEDrumming up sounds", "In the drumtown land", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEtypical qwerty", "with lousy shift key", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEnot@home.com", "telling you now", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"EEEi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());

		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFScott", "63215065", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFIshan", "Sahay", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFHazlett", "Hazlett", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFMarco", "Polo", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFOld", "MacDonald", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFreala.ece.ubc.ca", "QQorRQ", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFfinance hub of the world", "Abu Dhabi", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFGotta get up 1234", "Get up and rooool 5678", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFFFF iii FFF ii ooo", "sheep", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFbombastic", "is the word??", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFgettysburg address", "Where exactly?", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFDrumming up sounds", "In the drumtown land", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFtypical qwerty", "with lousy shift key", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFnot@home.com", "telling you now", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"FFFi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());

		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"GGGi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"HHHi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"IIIi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());
		populateOneTest(NodeCommands.Request.CMD_GET.getCode(), myCount+"JJJi can't change", "you can't see", NodeCommands.Reply.RPY_SUCCESS.getCode());
}

	public static void main(String[] args) {

		// If the command line arguments are missing, or invalid, then nothing to do
		if ( args.length != 2 ) {
			printUsage();
			return;
		}

		String serverURL = args[0];		
		int serverPort = -1;

		try {
			serverPort = Integer.parseInt(args[1]);
		} catch (NumberFormatException e1) {
			System.err.println("Invalid input.  Server Port and Student ID must be numerical digits only.");
			System.out.println("Invalid input.  Server Port and Student ID must be numerical digits only.");
			printUsage();
			return;
		}

		
		//hack to debug
		serverPort = 11112;
		
		List<Runnable> list = new LinkedList<Runnable>();

		// we are listening, so now allocated a ThreadPool to handle new sockets connections
		executor = Executors.newFixedThreadPool(NUM_THREADS_IN_POOL);

		for (int i=0; i<NUM_TEST_RUNNABLES; i++)
			list.add(new TestNode(serverURL, serverPort));

		if (!IS_BREVITY) System.out.println("-------------- Start Running Tests --------------");

		while (!list.isEmpty()) {
			executor.execute(list.remove(0));
		}

		synchronized(numTestsRunning) {
			while (numTestsRunning.get() > 0) {
				try {
					numTestsRunning.wait();
				} catch (InterruptedException e) { /* do nothing */}
			}
		}

		executor.shutdown();
		
		while (!executor.isTerminated()) { /* do nothing */ }

		if (!IS_BREVITY) System.out.println("-------------- Complete Running Tests --------------");
}

	public TestNode(String url, int port) {
		this.url = url;
		this.port = port;
	}

	@Override
	public void run() {
		Socket clientSocket = null;

		try {
			// There may be multiple IP addresses; but we only need one
			// so calling .getByName() is sufficient over .getAllByName()
			InetAddress address = InetAddress.getByName(url);
			if (!IS_BREVITY) System.out.println(
					"Connecting to: " 
							+ address.toString().replaceAll("/", " == ") 
							+ " on port " 
							+ port);

			// create a TCP socket to the server
			// Set a timeout on read operation as 3 seconds
			//			clientSocket = new Socket(serverURL, serverPort);

			populateTests();
			populateMemoryTests();
			//populateOneTest();

			// we will use this stream to send data to the server
			// we will use this stream to receive data from the server
			DataOutputStream outToServer = null;
			BufferedInputStream inFromServer = null;

			// Loop through all tests
			byte[] recvBuffer = new byte[NodeCommands.LEN_VALUE_BYTES];
			String replyString = null;
			boolean isPass;
			String failMessage = null;

			int thisTestPassed = 0;
			int thisTestFailed = 0;

			if (!IS_BREVITY) System.out.println("-------------- Start Running Test --------------");

			for (TestData test : tests) {
				isPass = true;

				if (IS_VERBOSE) System.out.println();
				if (!IS_BREVITY) System.out.println("--- Running Test: "+test);


				boolean tryAgain = true;
				while (tryAgain) {
					try {
						//Thread.sleep(1000);
						if (IS_VERBOSE) System.out.print("\t-Writing Test.");
						// initiate test with node by sending the test command
						clientSocket = new Socket(address, port);
						
						clientSocket.setSoTimeout(TCP_READ_TIMEOUT_MS);
						if (IS_VERBOSE) System.out.println("Connected to server ...");

						outToServer = new DataOutputStream(clientSocket.getOutputStream());
						inFromServer = new BufferedInputStream(clientSocket.getInputStream() );

						outToServer.write(test.buffer.array());

						// Code converting byte to hex representation obtained from
						// http://stackoverflow.com/questions/6120657/how-to-generate-a-unique-hash-code-for-string-input-in-android

						// get reply of one byte, and pretty format into "0xNN" string where N is the reply code
						int numBytesRead;

						if (IS_VERBOSE) System.out.print("-Reading Answer.");
						while ((numBytesRead = inFromServer.read(recvBuffer, 0, 1)) == 0 ) {}

						if ( numBytesRead > NodeCommands.LEN_CMD_BYTES ) {
							// did not receive the one byte reply that was expected.
							failMessage = "excess bytes reply.";
							isPass = false;
							tryAgain = false;

						} else if ( numBytesRead == -1 ) {
							// network error closed the socket prematurely
							throw new IOException();
						}
						replyString = "0x" + Integer.toString((recvBuffer[0] & 0xFF)+0x100, 16).substring(1);
						//expectedReplyString = "0x" + Integer.toString((test.replyCode & 0xFF)+0x100, 16).substring(1);
						//actualReplyString = "0x" + Integer.toString((recvBuffer[0] & 0xFF)+0x100, 16).substring(1);

						// Check the received reply against the expected reply and determine success of test
						if (recvBuffer[0] != test.replyCode) {
							if (NodeCommands.Reply.values()[recvBuffer[0]] == NodeCommands.Reply.RPY_OVERLOAD) {
								throw new IOException();
							}

							isPass = false;
							tryAgain = false;
							failMessage = "expected "+NodeCommands.Reply.values()[test.replyCode & 0xFF].toString()+" but instead "+ NodeCommands.Reply.values()[recvBuffer[0] & 0xFF].toString();
						}

						if (IS_VERBOSE) System.out.print("-Reading Value of GET.");
						int bytesRead = 0;
						int totalBytesRead = 0;
						// If test was a GET command, then additionally read pipe for reply and verify result
						if (isPass && NodeCommands.Request.CMD_GET.getCode() == test.cmd) {

							// we expect 1024 bytes of 'value' from this GET command
							while (bytesRead != -1 && inFromServer.available() > 0) {
								bytesRead = inFromServer.read(recvBuffer, totalBytesRead, NodeCommands.LEN_VALUE_BYTES - totalBytesRead);
								totalBytesRead += bytesRead;
							}

							if (recvBuffer[0] == NodeCommands.Reply.RPY_SUCCESS.getCode() && totalBytesRead != NodeCommands.LEN_VALUE_BYTES) {
								isPass = false;
								tryAgain = false;
								failMessage = "expected value "+test.value +
										" Number of bytes received: "+totalBytesRead;
							}
						}

						if (IS_VERBOSE) System.out.println("\tAbout socket: "+clientSocket.toString());
						if (IS_VERBOSE) System.out.println("\tSoTimeout: "+clientSocket.getSoTimeout()+
								", isClosed: "+clientSocket.isClosed()+
								", isInputShutdown: "+clientSocket.isInputShutdown()+
								", isOutputShutdown "+clientSocket.isOutputShutdown()			
								);

						// Made it this far without an IOException, so we do not need to retry this loop
						// set 'tryAgain' to false
						tryAgain = false;

						// Display result of test
						if (isPass) {
							if (!IS_BREVITY) System.out.println("*** TEST "+test.index+" PASSED - received reply "+replyString);
							thisTestPassed++;

						} else {
							System.err.println("### TEST FAILED - " + failMessage+ " for " + test.toString());
							System.out.println("Thread: "+myCount+"\n### TEST "+test.index+" FAILED - " + failMessage );
							thisTestFailed++;
						}
					} catch (BindException e) {
						if (clientSocket != null) {  
							try {
								clientSocket.close();
							} catch (IOException e1) { /* do nothing */ }
						}
						System.out.println("Failed to bind probably due to exahustion of available ports.  Sleeping for 2minutes.");
						System.err.println("Failed to bind probably due to exahustion of available ports.  Sleeping for 2minutes.");
						try {
							Thread.sleep(2 * 60000);
						} catch (InterruptedException e1) {}
						
					} catch (SocketTimeoutException e) {
						System.err.println("### "+ test.toString() + " " + failMessage);
						System.out.println("Thread: "+myCount+"\n### TEST "+test.index+" FAILED - " + failMessage);
						thisTestFailed++;

					} catch (IOException e) {
						// if (IS_VERBOSE) System.err.println("### network error, retrying in "+TIME_RETRY_MS+"ms "+ test.toString() + " " + failMessage);
						
						System.err.println("### network error, retrying in "+TIME_RETRY_MS+"ms "+ test.toString() + " " + failMessage);
						//e.printStackTrace();
						try {
							Thread.sleep(TIME_RETRY_MS);
						} catch (InterruptedException e1) {}

					} finally {
						if (clientSocket != null) {  
							try {
								clientSocket.close();
							} catch (IOException e) { /* do nothing */ }
						}
					}
				}
			}

			System.out.println("-------------- Passed/Fail = "+ testPassed.addAndGet(thisTestPassed)+"/"+testFailed.addAndGet(thisTestFailed) +" ------------------");
			if (IS_VERBOSE) System.out.println("-------------- Finished Running Tests --------------");

		} catch (UnknownHostException e) {
			System.out.println("Unknown Host.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Unknown IO Exception.");
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			System.out.println("Hashing algorithm not supported on this platform.");
			e.printStackTrace();
		} finally {
			announceDeath();
		}
	}


	@Override
	public void announceDeath() {
		synchronized(numTestsRunning) {
			numTestsRunning.decrementAndGet();
			numTestsRunning.notifyAll();
		}
	}
}
