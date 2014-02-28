package com.b6w7.eece411.P02;

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
import java.nio.charset.StandardCharsets;
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

	private static final int TCP_READ_TIMEOUT_MS = 3000;

	private static MessageDigest md;

	// list of test cases, each entry represeting one test case
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

	private static void populateTests() throws NoSuchAlgorithmException, UnsupportedEncodingException {

		md = MessageDigest.getInstance("SHA-1");

		byte cmd = -1;
		ByteBuffer hashedKey = null;
		ByteBuffer value = null;
		byte reply = -1;

		String keyString = null;
		String valueString = null;

		// we can reuse 'value' for both the sending and receiving of a GET operation,
		// so no need a buffer for the reply 1024 bytes

		// If we want to repeated append to hash before digesting, call update() 
		//			md.update("Scott".getBytes(StandardCharsets.UTF_8.displayName()));
		//			key.put(md.digest()); 

		// Excess bytes will be truncated when being placed into key which is 
		// limited to 32 bytes.  This is OK for our hashing.
		// However, we cannot truncate the value.  If the value exceeds our buffer then we 
		// capture the exception and skip that test.

		// test 1: put 'Scott' => '63215065' 
		try {
			keyString = "Scott";
			valueString = "63215065";

			hashedKey = ByteBuffer.allocate(NodeCommands.LEN_KEY_BYTES);
			value = ByteBuffer.allocate(NodeCommands.LEN_VALUE_BYTES);

			byte[] digest = md.digest(keyString.getBytes(StandardCharsets.UTF_8.displayName()));
			
			cmd = NodeCommands.CMD_PUT;
			if (digest.length > hashedKey.limit()) {
				hashedKey.put( digest, 0, hashedKey.limit() );
			} else {
				hashedKey.put( digest );
			}
			value.put(valueString.getBytes(StandardCharsets.UTF_8.displayName()));
			reply = NodeCommands.RPY_SUCCESS;
			tests.add(new TestData(cmd, hashedKey, value, reply, null));

		} catch (BufferOverflowException e) {
			System.out.println("test skipped for "+keyString+"=>"+valueString+"\nvalue exceeds "+NodeCommands.LEN_VALUE_BYTES+" bytes");
		}

		// test 2: put 'ssh-linux.ece.ubc.ca' => '137.82.52.29' 
		try {
			keyString = "ssh-linux.ece.ubc.ca";
			valueString = "137.82.52.29";

			hashedKey = ByteBuffer.allocate(NodeCommands.LEN_KEY_BYTES);
			value = ByteBuffer.allocate(NodeCommands.LEN_VALUE_BYTES);

			byte[] digest = md.digest(keyString.getBytes(StandardCharsets.UTF_8.displayName()));
			
			cmd = NodeCommands.CMD_PUT;
			if (digest.length > hashedKey.limit()) {
				hashedKey.put( digest, 0, hashedKey.limit() );
			} else {
				hashedKey.put( digest );
			}
			value.put(valueString.getBytes(StandardCharsets.UTF_8.displayName()));
			reply = NodeCommands.RPY_SUCCESS;
			tests.add(new TestData(cmd, hashedKey, value, reply, null));

		} catch (BufferOverflowException e) {
			System.out.println("test skipped for "+keyString+"=>"+valueString+"\nvalue exceeds "+NodeCommands.LEN_VALUE_BYTES+" bytes");
		}

		// test 3: put 'Ishan' => '60038106' 
		try {
			keyString = "Ishan";
			valueString = "60038106";

			hashedKey = ByteBuffer.allocate(NodeCommands.LEN_KEY_BYTES);
			value = ByteBuffer.allocate(NodeCommands.LEN_VALUE_BYTES);

			byte[] digest = md.digest(keyString.getBytes(StandardCharsets.UTF_8.displayName()));
			
			cmd = NodeCommands.CMD_PUT;
			if (digest.length > hashedKey.limit()) {
				hashedKey.put( digest, 0, hashedKey.limit() );
			} else {
				hashedKey.put( digest );
			}
			value.put(valueString.getBytes(StandardCharsets.UTF_8.displayName()));
			reply = NodeCommands.RPY_SUCCESS;
			tests.add(new TestData(cmd, hashedKey, value, reply, null));

		} catch (BufferOverflowException e) {
			System.out.println("test skipped for "+keyString+"=>"+valueString+"\nvalue exceeds "+NodeCommands.LEN_VALUE_BYTES+" bytes");
		}
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
			InputStream inFromServer = clientSocket.getInputStream();

			// Loop through all tests
			byte[] recvBuffer = new byte[NodeCommands.LEN_VALUE_BYTES];
			String replyString = null;
			String expectedReplyString = null;
			boolean isPass;
			String failMessage = null;

			System.out.println("-------------- Start Running Tests --------------");

			for (TestData test : tests) {
				isPass = true;

				System.out.println("--- Running Test: "+test);

				// initiate test with node by sending the test command
				outToServer.write(test.buffer.array());
				//StandardCharsets.UTF_8.displayName()

				try {

					// get reply of one byte, and pretty format into "0xNN" string where N is the reply code
					inFromServer.read(recvBuffer, 0, 1);
					replyString = "0x" + Integer.toString((recvBuffer[0] & 0xFF)+0x100).substring(1);
					expectedReplyString = "0x" + Integer.toString((test.replyCode & 0xFF)+0x100, 16).substring(1);

					// Check the received reply against the expected reply and determine success of test
					if (recvBuffer[0] != test.replyCode) {
						isPass = false;
						failMessage = "expected reply "+expectedReplyString;
					}

					// If test was a GET command, then additionally read pipe for reply and verify result
					if (isPass && NodeCommands.CMD_GET == test.cmd) {

						// we expect 1024 bytes of 'value' from this GET command
						int bytesRead = 0;
						int totalBytesRead = 0;
						while (bytesRead > -1) {
							bytesRead = inFromServer.read(recvBuffer, totalBytesRead, NodeCommands.LEN_VALUE_BYTES - totalBytesRead);
							totalBytesRead += bytesRead;
						}

						if (totalBytesRead != NodeCommands.LEN_VALUE_BYTES) {
							isPass = false;
							failMessage = "expected value "+test.value;
						}
					}

					if (isPass && (inFromServer.read() > -1)) {
						// So far so good, but let's make sure there is no more data on the socket.
						// If we read even one byte, then this is a failed test.
						isPass = false;
						failMessage = "excess bytes in pipe";
					}

					// Display result of test
					if (isPass) {
						System.out.println("*** TEST "+test.index+" PASSED - received reply "+replyString);
					} else {
						System.out.println("### TEST "+test.index+" FAILED - " + failMessage);
					}


				} catch (SocketTimeoutException e) {
					System.out.println("### TEST "+test.index+" FAILED - timeout on network operation");

				} catch (IOException e) {
					System.out.println("### TEST "+test.index+" FAILED - network error");

				} finally {
					try {
						while (inFromServer.read(recvBuffer, 0, recvBuffer.length) > -1) {
							// reagrdless of whether the test passed or failed,
							// we want to slurp the pipe so that the subsequent test will be unaffected
						}
					} catch (SocketTimeoutException e) {
						// ok.. squeltch
					}
				}
			}

			System.out.println("-------------- Finished Running Tests --------------");

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
