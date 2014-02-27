package com.b6w7.eece411.P02;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Launcher {
	
	private static final int BUFSIZE = 1+32+1024;   // Size of receive buffer

	public static void main(String[] args) throws IOException {
		int servPort = Integer.parseInt(args[0]);

		// Create a server socket to accept client connection requests
		ServerSocket servSock = new ServerSocket(servPort);

		int recvMsgSize;   // Size of received message
		byte[] byteBuffer = new byte[BUFSIZE];  // Receive buffer

		for (;;) { // Run forever, accepting and servicing connections
			Socket clntSock = servSock.accept();     // Get client connection

			System.out.println("Handling client at " +
					clntSock.getInetAddress().getHostAddress());

			InputStream in = clntSock.getInputStream();
			OutputStream out = clntSock.getOutputStream();

			// Receive until client closes connection, indicated by -1 return
			while ((recvMsgSize = in.read(byteBuffer)) != -1)
				out.write(byteBuffer, 0, recvMsgSize);

			clntSock.close();  // Close the socket.  We are done with this client!
		}
	}
}
