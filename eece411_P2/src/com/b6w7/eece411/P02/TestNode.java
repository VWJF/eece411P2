package com.b6w7.eece411.P02;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
/**
 * A test class for testing {@link Node}
 * 
 * @author Scott Hazlett
 * @author Ishan Sahay
 * @date 23 Jan 2014
 */
public class TestNode {

	private static final int TCP_READ_TIMEOUT_MS = 1000;

	private static MessageDigest md;

	private static int testPassed = 0;
	private static int testFailed = 0;
	// list of test cases, each entry representing one test case
	private static List<TestData> tests = new LinkedList<TestData>();

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

	
	private static void populateOneTest(byte cmd, String keyString, String valueString, byte reply) throws NoSuchAlgorithmException, UnsupportedEncodingException {
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
			//reply = NodeCommands.RPY_SUCCESS;
			
			if (NodeCommands.CMD_PUT == cmd) {
				// If we are performing a PUT, then we need to send value
				tests.add(new TestData(cmd, hashedKey, value, reply, null));
				
			} else if (NodeCommands.CMD_GET == cmd){
				// If we are performing a GET, then we do not have to send 'value', 
				// but we must see the value replied to us
				tests.add(new TestData(cmd, hashedKey, null, reply, value));
				
			} else if (NodeCommands.CMD_REMOVE == cmd){
				// If we are performing a REMOVE, then we do not have to send 'value',
				// and neither do we have to expect it as a returned value
				tests.add(new TestData(cmd, hashedKey, null, reply, null));
				
			} else {
				// Unrecognized Commands, the server should handle this case gracefully
				tests.add(new TestData(cmd, hashedKey, value, reply, null));
				//throw new IllegalArgumentException("Unknown command");
			}

		} catch (BufferOverflowException e) {
			System.out.println("test skipped for "+keyString+"=>"+valueString+"\nvalue exceeds "+NodeCommands.LEN_VALUE_BYTES+" bytes");
		}
	}
	
	private static void populateTests() throws NoSuchAlgorithmException, UnsupportedEncodingException {
		// test 1: put 'Scott' => '63215065', and so on ... 
		
		populateOneTest(NodeCommands.CMD_GET, "Scott", "63215065", NodeCommands.RPY_INEXISTENT);
		populateOneTest(NodeCommands.CMD_REMOVE, "Scott", "63215065", NodeCommands.RPY_INEXISTENT);
/*
		populateOneTest(NodeCommands.CMD_PUT, "Scott", "63215065", NodeCommands.RPY_SUCCESS);
		populateOneTest(NodeCommands.CMD_PUT, "Ishan", "Sahay", NodeCommands.RPY_SUCCESS);
		populateOneTest(NodeCommands.CMD_PUT, "ssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.RPY_SUCCESS);

		populateOneTest(NodeCommands.CMD_PUT, "John", "Smith", NodeCommands.RPY_OUT_OF_SPACE);

		populateOneTest(NodeCommands.CMD_GET, "Scott", "63215065", NodeCommands.RPY_SUCCESS);
		populateOneTest(NodeCommands.CMD_GET, "Ishan", "Sahay", NodeCommands.RPY_SUCCESS);
		populateOneTest(NodeCommands.CMD_GET, "ssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.RPY_SUCCESS);
		
		populateOneTest(NodeCommands.CMD_REMOVE, "Scott", "63215065", NodeCommands.RPY_SUCCESS);
		populateOneTest(NodeCommands.CMD_REMOVE, "Ishan", "Sahay", NodeCommands.RPY_SUCCESS);
		populateOneTest(NodeCommands.CMD_REMOVE, "ssh-linux.ece.ubc.ca", "137.82.52.29", NodeCommands.RPY_SUCCESS);
	
		populateOneTest(NodeCommands.CMD_GET, "Scott", "63215065", NodeCommands.RPY_INEXISTENT);
		populateOneTest(NodeCommands.CMD_GET, "Ishan", "Sahay", NodeCommands.RPY_INEXISTENT);
		
		populateOneTest(NodeCommands.CMD_GET, "localhost", "137.82.52.29", NodeCommands.RPY_INEXISTENT);
	*/
		
		populateOneTest(NodeCommands.CMD_UNRECOGNIZED, "Fake", "Fake", NodeCommands.RPY_UNRECOGNIZED_CMD);

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
			System.out.println("Invalid input.  Server Port and Student ID must be numerical digits only.");
			printUsage();
			return;
		}

		Socket clientSocket = null;

		try {
			// There may be multiple IP addresses; but we only need one
			// so calling .getByName() is sufficient over .getAllByName()
			InetAddress address = InetAddress.getByName(serverURL);
			System.out.println(
					"Connecting to: " 
							+ address.toString().replaceAll("/", " == ") 
							+ " on port " 
							+ serverPort);

			// create a TCP socket to the server
			// Set a timeout on read operation as 3 seconds
			clientSocket = new Socket(serverURL, serverPort);
			clientSocket.setSoTimeout(TCP_READ_TIMEOUT_MS);
			System.out.println("Connected to server ...");

			populateTests();

			// we will use this stream to send data to the server
			// we will use this stream to receive data from the server
			DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
			//InputStream inFromServer = clientSocket.getInputStream();
			BufferedInputStream inFromServer = new BufferedInputStream(clientSocket.getInputStream() );

			// Loop through all tests
			byte[] recvBuffer = new byte[NodeCommands.LEN_VALUE_BYTES];
			String replyString = null;
			String expectedReplyString = null;
			boolean isPass;
			String failMessage = null;

			System.out.println("-------------- Start Running Tests --------------");

			for (TestData test : tests) {
				isPass = true;
				
				System.out.println();
				System.out.println("--- Running Test: "+test);


				try {
					System.out.print("\t-Writing Test.");
					// initiate test with node by sending the test command
					outToServer.write(test.buffer.array());
					//StandardCharsets.UTF_8.displayName()

					// Code converting byte to hex representation obtained from
					// http://stackoverflow.com/questions/6120657/how-to-generate-a-unique-hash-code-for-string-input-in-android
					
					// get reply of one byte, and pretty format into "0xNN" string where N is the reply code
					int numBytesRead;
					
					System.out.print("-Reading Answer.");
					while ((numBytesRead = inFromServer.read(recvBuffer, 0, 1)) == 0 ) {}
					
					if ( numBytesRead > 1 ) {
						// did not receive the one byte reply that was expected.
						failMessage = "excess bytes reply.";
						isPass = false;
						
					} else if ( numBytesRead == -1 ) {
						// did not receive the one byte reply that was expected.
						failMessage = "broken pipe";
						isPass = false;
						
					}
					replyString = "0x" + Integer.toString((recvBuffer[0] & 0xFF)+0x100, 16).substring(1);
					expectedReplyString = "0x" + Integer.toString((test.replyCode & 0xFF)+0x100, 16).substring(1);

					// Check the received reply against the expected reply and determine success of test
					if (recvBuffer[0] != test.replyCode) {
						isPass = false;
						failMessage = "expected reply "+expectedReplyString;
					}

					System.out.print("-Reading Value of GET.");
					// If test was a GET command, then additionally read pipe for reply and verify result
					if (isPass && NodeCommands.CMD_GET == test.cmd) {

						// we expect 1024 bytes of 'value' from this GET command
						int bytesRead = 0;
						int totalBytesRead = 0;
						while (bytesRead != -1 && inFromServer.available() > 0) {
							bytesRead = inFromServer.read(recvBuffer, totalBytesRead, NodeCommands.LEN_VALUE_BYTES - totalBytesRead);
							totalBytesRead += bytesRead;
						}

						if (recvBuffer[0] == NodeCommands.RPY_SUCCESS && totalBytesRead != NodeCommands.LEN_VALUE_BYTES) {
							isPass = false;
							failMessage = "expected value "+test.value +
												" Number of bytes received: "+totalBytesRead;
						}
					}

					System.out.print("-Reading Excess byte in pipe. \n");
					try {
						if (isPass && (inFromServer.read() > 0)) {
							// So far so good, but let's make sure there is no more data on the socket.
							// If we read even one byte, then this is a failed test.
							isPass = false;
							failMessage = "excess bytes in pipe";
						}
					} catch (SocketTimeoutException e) {
						// read() is a blocking operation, and we did not find any more bytes in the pipe
						// so we are satisfied that the test passed.  do nothing here.
					}
					
					System.out.println("\tAbout socket: "+clientSocket.toString());
					System.out.println("\tSoTimeout: "+clientSocket.getSoTimeout()+
										", isClosed: "+clientSocket.isClosed()+
										", isInputShutdown: "+clientSocket.isInputShutdown()+
										", isOutputShutdown "+clientSocket.isOutputShutdown()			
										);
					
					// Display result of test
					if (isPass) {
						System.out.println("*** TEST "+test.index+" PASSED - received reply "+replyString);
						testPassed++;
					} else {
						System.out.println("### TEST "+test.index+" FAILED - " + failMessage);
						testFailed++;
					}


				} catch (SocketTimeoutException e) {
					System.out.println("### TEST "+test.index+" FAILED - timeout on network operation");
					testFailed++;

				} catch (IOException e) {
					System.out.println("### TEST "+test.index+" FAILED - network error");
					testFailed++;

				} finally {
					try {
						while (inFromServer.read(recvBuffer, 0, recvBuffer.length) > -1) {
							// regardless of whether the test passed or failed,
							// we want to slurp the pipe so that the subsequent test will be unaffected
						}
					} catch (SocketTimeoutException e) {
						// ok.. squeltch
					}
				}
			}

			System.out.println("-------------- Finished Running Tests --------------");
			System.out.println("-------------- Passed/Fail = "+ testPassed+"/"+testFailed +" ------------------");


			if (clientSocket != null)
				clientSocket.close();

		} catch (UnknownHostException e) {
			System.out.println("Unknown Host.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Unknown IO Exception.");
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			System.out.println("Hashing algorithm not supported on this platform.");
			e.printStackTrace();
		}
	}
}
