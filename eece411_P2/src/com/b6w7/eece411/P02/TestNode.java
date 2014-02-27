/*
 * A client to connect to the EECE 411 Assignment #1 Server
 * and obtain the "new" student ID.  
 * The server operats on Little-Endian, whilst Java natively
 * runs on Big-Endian.
 * 
 * @author Scott Hazlett
 * @date 23 Jan 2014
 */

package com.b6w7.eece411.P02;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class TestNode {

	// TCP socket connection code obtained and modified from
	// http://systembash.com/content/a-simple-java-tcp-server-and-tcp-client/
	// "A Simple Java TCP Server and TCP Client"

	private static final int SECRET_CODE_LENGTH_OFF = 8;
	private static int secretCodeLength = 0;
	private static int msgLength = 0;

	private static void printUsage() {
		System.out.println("USAGE:\n"
				+ "  java -cp"
				+ " <file.jar>"
				+ " " + TestNode.class.getCanonicalName() 
				+ " <server URL>"  
				+ " <server port>"  
				+ " <student ID>");
		System.out.println("EXAMPLE:\n"
				+ "  java -cp"
				+ " A1.jar"
				+ " " + TestNode.class.getCanonicalName() 
				+ " reala.ece.ubc.ca"  
				+ " 5627"  
				+ " 909090");
	}
	
	public static void main(String[] args) {
		// If the command line arguments are missing, then nothing to do
		if ( args.length != 3 ) {
			printUsage();
			return;
		}

		String serverURL = args[0];
		int serverPort = -1;
		int studentID = -1;
		
		try {
			serverPort = Integer.parseInt(args[1]);
			studentID = Integer.parseInt(args[2]);
		} catch (NumberFormatException e1) {
			System.out.println("Invalid input.  Server Port and Student ID must be numerical digits only.");
			printUsage();
			return;
		}

		Socket clientSocket = null;
		ByteBuffer b = null;
		
		
		try {
			// URL resolution and InetAddress resolution code obtained and modified from 
			// http://stackoverflow.com/questions/9286861/get-ip-address-with-url-string-java
			// "Get IP address with URL string? (Java)"

			// There may be multiple IP addresses; but we only need one
			// so calling .getByName() is sufficient over .getAllByName()
			InetAddress address = InetAddress.getByName(serverURL);
			System.out.println(
					"Connecting to: " 
							+ address.toString().replaceAll("/", " == ") 
							+ " on port " 
							+ serverPort
							+ " with studentID "
							+ studentID);

			// create a TCP socket to the server
			clientSocket = new Socket(serverURL, serverPort);
			System.out.println("Connected to server ...");
			
			// we will use this stream to send data to the server
			// we will use this stream to receive data from the server
			DataOutputStream outToServer = 
					new DataOutputStream(clientSocket.getOutputStream());
			BufferedReader inFromServer = 
					new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

			// Create buffer and specify LE ordering before we insert into buffer
			// Read one integer from the LE-ordered server and place into the buffer
			//b = ByteBuffer.allocate(4);
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
			b = ByteBuffer.allocate(msgLength);
			//b.order(java.nio.ByteOrder.LITTLE_ENDIAN);
			//b.putInt(0xDEADBEEF);
			for (int ii = 4; ii < msgLength; ii += 4)
				b.putInt(readOneIntFromBufferedReader(inFromServer));
			
			// switch the buffer back to BE and 
			// read the secret code length from the buffer
			b.order(java.nio.ByteOrder.BIG_ENDIAN);
			secretCodeLength = b.getInt(SECRET_CODE_LENGTH_OFF);
			System.out.println("Code length: " + secretCodeLength);
			
			// The location of the code is
			// msgLength - secretCodeLength - EOM_flag
			// we switch back to LE because the assignment output
			// uses LE.  
			// *** If BE was desired, then we would use BIG_ENDIAN here ***
			b.order(java.nio.ByteOrder.LITTLE_ENDIAN);
			System.out.print("Got secret: " );
			for (int ii = msgLength - secretCodeLength - 4; ii < msgLength - 4; ii += 4)
				System.out.format("%08X ", b.getInt(ii));
			System.out.println();
			
			if (clientSocket != null)
				clientSocket.close();

		} catch (UnknownHostException e) {
			System.out.println("Unknown Host.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Unknown IO Exception.");
			e.printStackTrace();
		}
	}
}
