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

import com.b6w7.eece411.P02.multithreaded.NodeCommands.Request;

public class WorkerThread implements Runnable {

	// extra debug output from normal
	private static boolean IS_VERBOSE = false;
		
	private final Socket socket;
	private final PostCommand db;
	private final Map<ByteArrayWrapper, byte[]> map;
	private final JoinThread parent;

	// number of bytes in protocol field
	private static final int CMDSIZE = NodeCommands.LEN_CMD_BYTES;		
	private static final int KEYSIZE = NodeCommands.LEN_KEY_BYTES;
	private static final int VALUESIZE = NodeCommands.LEN_VALUE_BYTES;

	// Size of protocol buffers
	private static final int REQSIZE = CMDSIZE+KEYSIZE+VALUESIZE;  // request buffer
	private static final int RESSIZE = CMDSIZE+VALUESIZE;   //response buffer

	private Command cmd = null;

	public boolean keepRunning = true;

	/**
	 * Constructor for a WorkerThread that services a client socket request
	 * @param socket to the client
	 * @param parent interface to announce that the socket has been closed
	 */
	public WorkerThread(Socket socket, PostCommand db, Map<ByteArrayWrapper, byte[]> map, JoinThread parent) {
		System.out.println("Instantiating WorkerThread for service");
		// TODO check for null
		this.socket = socket;
		this.db = db;
		this.map = map;
		this.parent = parent;
	}

	/**
	 * Constructor for a WorkerThread that does not service a client, rather, 
	 * only replies to the client that the server is overloaded
	 * @param socket to the client
	 * @param parent interface to announce that the socket has been closed
	 */
	public WorkerThread(Socket socket, JoinThread parent) {
		System.out.println("Instantiating WorkerThread for rejection");
		// TODO check for null
		this.socket = socket;
		this.map = null;
		this.db = null;
		this.parent = parent;
	}

	@Override
	public void run() {
		int recvMsgSize = 0;   // Size of received message
		int totalBytesReceived = 0;   // Size of received message

		byte[] byteBufferIn = new byte[REQSIZE];  // Receive buffer
		byte[] byteBufferOut = new byte[RESSIZE]; // Response buffer

		try {

			DataOutputStream outToClient = 
					new DataOutputStream(socket.getOutputStream());
			BufferedInputStream inFromClient = 
					new BufferedInputStream(socket.getInputStream() );

			if (map == null) {
				// if map is null then the WorkerThread only needs to reject the client connections
				// with the rejection code
				byteBufferOut[0] = NodeCommands.Reply.RPY_OVERLOAD.getCode();
				outToClient.write(byteBufferOut, 0, 1);
			} else {

				long timeStart = new Date().getTime();

				if (IS_VERBOSE) System.out.print("--Parsing Command  ");
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

				if (IS_VERBOSE) System.out.print("--Parsing Key  ");
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
					if (IS_VERBOSE) System.out.print("--Parsing Value  ");
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
					if (IS_VERBOSE) System.out.println("\nSuccessfully read CMD+KEY+VALUE... "+totalBytesReceived+" bytes");

				} else {
					if (IS_VERBOSE) System.out.println("\nSuccessfully read CMD+KEY... "+totalBytesReceived+" bytes");
				}

				dataRead = ByteBuffer.wrap(	byteBufferIn );
				value = Arrays.copyOfRange(dataRead.array(), CMDSIZE+KEYSIZE, CMDSIZE+KEYSIZE+VALUESIZE);

				String s = NodeCommands.requestByteArrayToString(dataRead.array());
				if (IS_VERBOSE) System.out.println("Request Received(cmd,key,value): "+s.toString());
				// redundant println:
				//System.out.println("Request Received(cmd,key,value): ("+cmdByte+", "+key+", "+value.toString()+") ");

				if ((int)cmdByte >= Request.values().length || (int)cmdByte < 0 )
					cmdByte = Request.CMD_UNRECOG.getCode();

				switch (Request.values()[cmdByte]) {
				case CMD_PUT:
					cmd = new PutCommand(key, value, map);
					//System.out.println("Issuing "+cmd);
					db.post(cmd);
					break;				
				case CMD_GET:
					cmd = new GetCommand(key, map);
					//System.out.println("Issuing:  "+cmd);
					db.post(cmd);
					break;				
				case CMD_REMOVE:
					cmd = new RemoveCommand(key, map);
					//System.out.println("Issuing:  "+cmd);
					db.post(cmd);
					break;		
				default:
					cmd = new UnrecognizedCommand();
					//System.out.println("Issuing:  "+cmd);

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
					throw new IOException("Timeout on reply from database. TotalBytesRead = " + totalBytesReceived + " ResultReady: "+resultReady);
				}

				// Send reply to client
				if( socket != null){
					if (IS_VERBOSE) System.out.println("Writing Response.");

					outToClient.write(cmd.getReply());
					
					byteBufferOut = cmd.getReply();
					//System.out.println("Replying: "+cmd);
					
//					String p = new String(byteBufferOut, "UTF-8");
//					String q = NodeCommands.byteArrayAsString(byteBufferOut);
//					outToClient.write(byteBufferOut, 0, byteBufferOut.length);
					
					// System.out.println("Total elements in map: "+ map.size());
					//	System.out.println("Total elements in map: "+ Command.getNumElements());
					//  System.out.println("All Bytes Written(array): ( "+q.substring(0, 2)+" "+q.substring(2)+")");
					if (IS_VERBOSE) System.out.println("Expected Bytes in response, Total Bytes written in socket: (" + byteBufferOut.length+ ", " +outToClient.size()+")");
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

			}
		} catch (IOException e1) {
			// Error in reading and writing to output stream.
			if (IS_VERBOSE) System.out.println("Socket exception when reading/sending data.");
			
		} finally {

			if(socket != null){
				try{	
					if (IS_VERBOSE) System.out.println("\tAbout socket: "+socket.toString());

					if (IS_VERBOSE) System.out.println("\tSoTimeout: "+socket.getSoTimeout()+
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
					if (IS_VERBOSE) System.out.println("Closing socket. Written bytes: "+byteBufferOut.length);
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		parent.announceDeath();
	}
}

