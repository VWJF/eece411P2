package com.b6w7.eece411.P02;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Launcher {
	
	private static final int BUFSIZE = 2*(1+32+1024);   // Size of receive buffer

	enum states {
		CMD, KEY, VALUE;
	}
	
	public static void main(String[] args) throws IOException {
		int servPort = Integer.parseInt(args[0]);
		states state = states.CMD;

		// Create a server socket to accept client connection requests
		ServerSocket servSock = new ServerSocket(servPort);

		System.out.println("Listening for connections...");

		int recvMsgSize = 0;   // Size of received message
		byte[] byteBuffer = new byte[BUFSIZE];  // Receive buffer
		byte[] outByteBuffer = new byte[BUFSIZE];
		
		outByteBuffer[0] = NodeCommands.RPY_SUCCESS;

		for (;;) { // Run forever, accepting and servicing connections
			Socket clntSock = servSock.accept();     // Get client connection

			System.out.println("Handling client at " +
					clntSock.getInetAddress().getHostAddress());

			InputStream in = clntSock.getInputStream();
			OutputStream out = clntSock.getOutputStream();
			boolean wasStateUpdate = false;

			// Receive until client closes connection, indicated by -1 return
			int totalRecvMsgSize = 0;
			while ((recvMsgSize = in.read(byteBuffer)) != -1) {
				System.out.println("received:" + new String(byteBuffer, 0, recvMsgSize, StandardCharsets.UTF_8.displayName()));
				totalRecvMsgSize += recvMsgSize;
				
				// update state machine so we can correctly handle the inbound buffer
				do {
					wasStateUpdate = false;
					
					switch (state) {
					case CMD:
						if (totalRecvMsgSize > 0) {
							System.out.println("State changed from CMD -> KEY");
							state = states.KEY;
							wasStateUpdate = true;
						}
						break;
						
					case KEY:
						if (totalRecvMsgSize > 32) {
							System.out.println("State changed from KEY -> VALUE");
							out.write(outByteBuffer, 0, 1);  /// hard-coded success
							state = states.VALUE;
							wasStateUpdate = true;
						}
						break;
						
					case VALUE:
						if (totalRecvMsgSize > (1024+32)) {
							System.out.println("State changed from VALUE -> CMD");
							// a complete command came from client.  Time to expunge our local buffer
							byte[] temp = Arrays.copyOfRange(byteBuffer, (1+1024+32), (1+2024+32+BUFSIZE));
							byteBuffer = temp;
							totalRecvMsgSize -= (1+1024+32);
							state = states.CMD;
							wasStateUpdate = true;
						}
						break;
					default:
						break;
					}
				} while (wasStateUpdate);

				System.out.println("[recvMsgSize, totalRecvMsgSize] = ["+recvMsgSize+","+totalRecvMsgSize+"]");
			}

//			// Receive until client closes connection, indicated by -1 return
//			while ((recvMsgSize = in.read(byteBuffer)) != -1)
//				out.write(byteBuffer, 0, recvMsgSize);

			System.out.println("Closing socket.");
			clntSock.close();  // Close the socket.  We are done with this client!
		}
	}
}
