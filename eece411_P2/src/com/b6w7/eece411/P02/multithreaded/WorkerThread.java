package com.b6w7.eece411.P02.multithreaded;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import com.b6w7.eece411.P02.NodeCommands;
import com.b6w7.eece411.P02.NodeCommands.Request;

public class WorkerThread extends Thread {

	private final Socket socket;
	private final PostCommand db;
	private final Map<String, String> map;

	// number of bytes in protocol field
	private static final int CMDSIZE = NodeCommands.LEN_CMD_BYTES;		
	private static final int KEYSIZE = NodeCommands.LEN_KEY_BYTES;
	private static final int VALUESIZE = NodeCommands.LEN_VALUE_BYTES;

	// Size of protocol buffers
	private static final int REQSIZE = CMDSIZE+KEYSIZE+VALUESIZE;  // request buffer
	private static final int RESSIZE = CMDSIZE+VALUESIZE;   //response buffer

	private Command cmd = null;

	public boolean keepRunning = true;

	public WorkerThread(Socket socket, PostCommand db, Map<String, String> map) {
		System.out.println("Instantiating WorkerThread");
		// TODO check for null
		this.socket = socket;
		this.db = db;
		this.map = map;
	}

	@Override
	public void run() {
		int recvMsgSize = 0;   // Size of received message
		int totalBytesReceived = 0;   // Size of received message

		byte[] byteBufferIn = new byte[REQSIZE];  // Receive buffer
		byte[] byteBufferOut = new byte[RESSIZE]; // Response buffer

		int attempt = 0;

		try {

			DataOutputStream outToClient = 
					new DataOutputStream(socket.getOutputStream());
			BufferedInputStream inFromClient = 
					new BufferedInputStream(socket.getInputStream() );


			long timeStart = new Date().getTime();

			System.out.println("Parsing Command");
			// We want to decode the command
			// We try to get CMDSIZE number of bytes from pipe to decode the command
			// retrying at 100ms intervals
			// with a total timeout of 5000ms
			do {
				recvMsgSize = inFromClient.read(byteBufferIn
						, totalBytesReceived
						, CMDSIZE - totalBytesReceived);
				totalBytesReceived += recvMsgSize;
				if (totalBytesReceived < CMDSIZE)
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) { /* do nothing */ }
			} while (((new Date().getTime() - timeStart) < 5000) && totalBytesReceived < CMDSIZE);

			// if we did not receive the command within the time frame, throw exception.
			if (totalBytesReceived < CMDSIZE) {
				throw new IOException("Timeout on channel.  TotalBytesRead = " + totalBytesReceived);
			}

			System.out.println("Parsing Key");
			// we have successfully parsed the command
			// next, we want to decode the key
			// We try to get KEYSIZE number of bytes from pipe to decode the command
			// retrying at 100ms intervals
			// with a total timeout of 5000ms
			do {
				recvMsgSize = inFromClient.read(byteBufferIn
						, totalBytesReceived
						, KEYSIZE + CMDSIZE - totalBytesReceived);
				totalBytesReceived += recvMsgSize;
				if (totalBytesReceived < KEYSIZE + CMDSIZE)
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) { /* do nothing */ }
			} while (((new Date().getTime() - timeStart) < 5000) && totalBytesReceived < CMDSIZE);

			// if we did not receive the command within the time frame, throw exception.
			if (totalBytesReceived < CMDSIZE + KEYSIZE) {
				throw new IOException("Timeout on channel.  TotalBytesRead = " + totalBytesReceived);
			}

			//Parse and Extract relevant data -- cmd and key 
			byte cmdByte;
			byte[] key;
			byte[] value;

			ByteBuffer dataRead = ByteBuffer.wrap(byteBufferIn);
			cmdByte = dataRead.get();
			key = Arrays.copyOfRange(dataRead.array(), CMDSIZE, CMDSIZE+KEYSIZE);

			// next, only if the command is a put, we also want to decode the value 
			// We try to get VALUESIZE number of bytes from pipe to decode the command
			// retrying at 100ms intervals
			// with a total timeout of 5000ms
			if( cmdByte == (byte) Request.CMD_PUT.getCode() ){
				System.out.println("Parsing Value");
				do {
					recvMsgSize = inFromClient.read(byteBufferIn
							, totalBytesReceived
							, VALUESIZE + KEYSIZE + CMDSIZE - totalBytesReceived);
					totalBytesReceived += recvMsgSize;
					if (totalBytesReceived < VALUESIZE + KEYSIZE + CMDSIZE)
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) { /* do nothing */ }
				} while (((new Date().getTime() - timeStart) < 5000) && totalBytesReceived < CMDSIZE);

				// if we did not receive the command within the time frame, throw exception.
				if (totalBytesReceived < VALUESIZE + KEYSIZE + CMDSIZE) {
					throw new IOException("Timeout on channel.  TotalBytesRead = " + totalBytesReceived);
				}

				// we have successfully parsed the command, the key, and the value
				System.out.println("Sucessfully read CMD+KEY+VALUE... "+totalBytesReceived+" bytes");

			} else {
				System.out.println("Sucessfully read CMD+KEY... "+totalBytesReceived+" bytes");
			}

			dataRead = ByteBuffer.wrap(	byteBufferIn );
			value = Arrays.copyOfRange(dataRead.array(), CMDSIZE+KEYSIZE, CMDSIZE+KEYSIZE+VALUESIZE);

			String s = NodeCommands.requestByteArrayToString(dataRead.array());
			System.out.println("Request Received(cmd,key,value): "+s.toString());
			System.out.println("Request Received(cmd,key,value): ("+cmdByte+", "+key+", "+value.toString()+") ");

			switch (cmdByte) {
			case NodeCommands.CMD_PUT:
				cmd = new PutCommand(cmdByte, ByteBuffer.wrap(key), ByteBuffer.wrap(value), map);
				db.post(cmd);
				break;				
			case NodeCommands.CMD_GET:
				cmd = new PutCommand(cmdByte, ByteBuffer.wrap(key), ByteBuffer.wrap(value), map);
				db.post(cmd);
				break;				
			case NodeCommands.CMD_REMOVE:
				cmd = new PutCommand(cmdByte, ByteBuffer.wrap(key), ByteBuffer.wrap(value), map);
				db.post(cmd);
				break;		
			default:
				cmd = new UnrecognizedCommand();

			}

			//Obtain a connected client and reply to the client with its response.
			//Command clientToReply =	connected_clients.poll();

			// We have sent the command to be processed,
			// now we wait for asynchronous reply.
			// We poll for result, then send it along wire
			// retrying at 100ms intervals
			// with a total timeout of 5000ms
			boolean resultReady = false;
			do {
				synchronized (cmd.execution_completed) {
					resultReady = cmd.execution_completed;
				}
			} while (resultReady == false && ((new Date().getTime() - timeStart) < 5000));

			// if we did not receive the command within the time frame, throw exception.
			if (!resultReady) {
				throw new IOException("Timeout on reply from database.  TotalBytesRead = " + totalBytesReceived);
			}

			// Send reply to client
			if( socket != null){
				System.out.println("Writing Response.");

				byteBufferOut= cmd.getReply().array();
				String p = new String(byteBufferOut);
				String q = NodeCommands.byteArrayAsString(byteBufferOut);
				outToClient.write(byteBufferOut, 0, byteBufferOut.length);
				//								System.out.println("Total elements in map: "+ Command.getNumElements());
				//								System.out.println("Total elements in map: "+ Command.getNumElements());
				System.out.println("All Bytes Written(string,array): ("+ p+", "+q.substring(0, 2)+" "+q.substring(2)+")");
				System.out.println("Expected Bytes in response, Total Bytes written in socket: (" + p.length()+ ", " +outToClient.size()+")");

			} 
			//System.out.println("Closing socket. Written bytes: "+byteBufferOut.length);
			//clientToReply.getSocket().close(); // Close the socket.  We are done with this client!

			//				System.out.println("Completed Processing.");
			//
			//				System.out.println("\tAbout socket: "+socket.toString());
			//				System.out.println("\tSoTimeout: "+socket.getSoTimeout()+
			//						", isClosed: "+socket.isClosed()+
			//						", isInputShutdown: "+socket.isInputShutdown()+
			//						", isOutputShutdown "+socket.isOutputShutdown()+
			//						", getSendBufferSize "+socket.getSendBufferSize()+
			//						", getReceiveBufferSize "+socket.getReceiveBufferSize()
			//						);


			// TODO write appropriate data depending on result of operation
			// one idea: have another method similar to Command.execute(),
			// e.g. Command.replyExecute(), which would generate the byte sequence
			// that should be sent down the pipe, and then have this method actually
			// send it.
			// Another idea: have a switch statement, and check for the type of class
			// of the command.  Depending on if it is PutCommand, GetCommand, or 
			// RemoveCommand, as well as the value of the replyCode, react accordingly.

			// 2nd idea has disadvantage that there is tight coupling between the 
			// workerThread and the logic of the Command, but may be simpler.  Not sure.


		} catch (IOException e1) {
			// TODO: remove printStackTrace()
			// Error in reading and writing to output stream.
			e1.printStackTrace();
			System.out.println("Socket exception when reading/sending data.");
		} finally{

			if(socket != null){
				try{	
					System.out.println("\tAbout socket: "+socket.toString());

					System.out.println("\tSoTimeout: "+socket.getSoTimeout()+
							", isClosed: "+socket.isClosed()+
							", isInputShutdown: "+socket.isInputShutdown()+
							", isOutputShutdown "+socket.isOutputShutdown()+
							", getSendBufferSize "+socket.getSendBufferSize()+
							", getReceiveBufferSize "+socket.getReceiveBufferSize()
							);
				} catch(SocketException se){
					se.printStackTrace();
				}

				try{	
					System.out.println("Closing socket. Written bytes: "+byteBufferOut.length);
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}

