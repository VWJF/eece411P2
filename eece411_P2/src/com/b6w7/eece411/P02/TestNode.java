package com.b6w7.eece411.P02;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.Socket;
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
	
	private static MessageDigest md;
	
	// list of test cases, each entry represeting one test case
	private static List<TestData> tests = new LinkedList<TestData>();

	// Allocate enough buffer for length of one 'value' in a transmit
	private static ByteBuffer buffer = ByteBuffer.allocate(1024);
	
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
		ByteBuffer hashedKey = ByteBuffer.allocate(32);
		ByteBuffer value = ByteBuffer.allocate(1024);
		byte errorCode = -1;
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
			
			cmd = NodeCommands.CMD_PUT;
			hashedKey.put(md.digest(keyString.getBytes(StandardCharsets.UTF_8.displayName())), 0, hashedKey.limit());
			value.put(valueString.getBytes(StandardCharsets.UTF_8.displayName()));
			reply = NodeCommands.RPY_SUCCESS;
			tests.add(new TestData(cmd, hashedKey, value, reply, null));
			
		} catch (BufferOverflowException e) {
			System.out.println("test skipped for "+keyString+"=>"+valueString+"\nvalue exceeds 1024 bytes");
		}
		
		hashedKey.clear(); 
		value.clear();

		// test 2: put 'ssh-linux.ece.ubc.ca' => '63215065' 
		try {
			keyString = "ssh-linux.ece.ubc.ca";
			valueString = "137.82.52.29";
			
			cmd = NodeCommands.CMD_PUT;
			hashedKey.put(md.digest(keyString.getBytes(StandardCharsets.UTF_8.displayName())), 0, hashedKey.limit());
			value.put(valueString.getBytes(StandardCharsets.UTF_8.displayName()));
			reply = NodeCommands.RPY_SUCCESS;
			tests.add(new TestData(cmd, hashedKey, value, reply, null));
			
		} catch (BufferOverflowException e) {
			System.out.println("test skipped for "+keyString+"=>"+valueString+"\nvalue exceeds 1024 bytes");
		}
		
		hashedKey.clear(); 
		value.clear();

		// test 3: put 'Ishan' => 'Sahay' 
		try {
			keyString = "Scott";
			valueString = "63215065";
			cmd = NodeCommands.CMD_PUT;
			hashedKey.put(md.digest(keyString.getBytes(StandardCharsets.UTF_8.displayName())), 0, hashedKey.limit());
			value.put(valueString.getBytes(StandardCharsets.UTF_8.displayName()));
			reply = NodeCommands.RPY_SUCCESS;
			
			tests.add(new TestData(cmd, hashedKey, value, reply, null));
			
		} catch (BufferOverflowException e) {
			System.out.println("test skipped for "+keyString+"=>"+valueString+"\nvalue exceeds 1024 bytes");
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
			clientSocket = new Socket(serverURL, serverPort);
			System.out.println("Connected to server ...");
			
			populateTests();
			
			// we will use this stream to send data to the server
			// we will use this stream to receive data from the server
			DataOutputStream outToServer = 
					new DataOutputStream(clientSocket.getOutputStream());
			BufferedReader inFromServer = 
					new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

			
			// Test 1
			buffer.putInt(NodeCommands.CMD_PUT);
			
			// Create buffer and specify LE ordering before we insert into buffer
			// Read one integer from the LE-ordered server and place into the buffer
			buffer = ByteBuffer.allocate(4);
			//b.order(java.nio.ByteOrder.LITTLE_ENDIAN);
			//b.putInt(readOneIntFromBufferedReader(inFromServer));

			// reset cursor to start and
			// specify BE ordering
			// and read out the message length as one BE int
			//b.rewind();
			//b.order(java.nio.ByteOrder.BIG_ENDIAN);
			//msgLength = b.getInt();
			//System.out.println("Message length: " + msgLength);

			// now that we have the size of the reply from the server, 
			// we can allocate an appropriately-sized buffer for the reply
			// for index clarity, we re-insert a dummy int for the message size
			// then we read the remaining data from the server into this buffer
			//buffer = ByteBuffer.allocate(msgLength);
			//b.order(java.nio.ByteOrder.LITTLE_ENDIAN);
			//b.putInt(0xDEADBEEF);
//			for (int ii = 4; ii < msgLength; ii += 4)
//				b.putInt(readOneIntFromBufferedReader(inFromServer));
//			
			// switch the buffer back to BE and 
			// read the secret code length from the buffer
			buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
//			secretCodeLength = b.getInt(SECRET_CODE_LENGTH_OFF);
//			System.out.println("Code length: " + secretCodeLength);
			
			// The location of the code is
			// msgLength - secretCodeLength - EOM_flag
			// we switch back to LE because the assignment output
			// uses LE.  
			// *** If BE was desired, then we would use BIG_ENDIAN here ***
			buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
			System.out.print("Got secret: " );
//			for (int ii = msgLength - secretCodeLength - 4; ii < msgLength - 4; ii += 4)
//				System.out.format("%08X ", b.getInt(ii));
			System.out.println();
			
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
