package com.b6w7.eece411.P02;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Launcher {

	private static final int BUFSIZE = 2*(1+32+1024); // Size of receive buffer

	enum states {
		CMD, KEY, VALUE;
	}

	public static void main(String[] args) throws IOException {
		int servPort = Integer.parseInt(args[0]);
		states state = states.CMD;

		// Create a server socket to accept client connection requests
		ServerSocket servSock = new ServerSocket(servPort);

		System.out.println("Listening for connections...");

		int recvMsgSize = 0; // Size of received message
		byte[] byteBuffer = new byte[BUFSIZE]; // Receive buffer
		byte[] outByteBuffer = new byte[BUFSIZE];

		outByteBuffer[0] = NodeCommands.RPY_SUCCESS;

		for (;;) { // Run forever, accepting and servicing connections
			Socket clntSock = servSock.accept(); // Get client connection

			System.out.println("Handling client at " +
					clntSock.getInetAddress().getHostAddress());

			InputStream in = clntSock.getInputStream();
			OutputStream out = clntSock.getOutputStream();
			boolean wasStateUpdate = false;

			// Receive until client closes connection, indicated by -1 return
			int totalRecvMsgSize = 0;
			while ((recvMsgSize = in.read(byteBuffer, totalRecvMsgSize, BUFSIZE - totalRecvMsgSize)) != -1) {

				totalRecvMsgSize += recvMsgSize;

				System.out.println("[recvMsgSize, totalRecvMsgSize] = ["+recvMsgSize+","+totalRecvMsgSize+"]");

				// update state machine so we can correctly handle the inbound buffer
				do {
					wasStateUpdate = false;

					switch (state) {
					case CMD:
						if (totalRecvMsgSize > 0) {
							System.out.println("Received Cmd: " + Integer.toString((byteBuffer[0] & 0xff) + 0x100, 16).substring(1));
							System.out.println("--- State changed from CMD -> KEY");
							state = states.KEY;
							wasStateUpdate = true;
						}
						break;

					case KEY:
						if (totalRecvMsgSize > 32) {
							StringBuilder s = new StringBuilder();
							for (int i=1; i<(1+32); i++) {
								s.append(Integer.toString((byteBuffer[i] & 0xff) + 0x100, 16).substring(1));
							}
							System.out.println("Received Key: " + s.toString());
							System.out.println("--- State changed from KEY -> VALUE");
							state = states.VALUE;
							wasStateUpdate = true;
						}
						break;

					case VALUE:
						if (totalRecvMsgSize > (1024+32)) {
							System.out.println("Received Value:" + new String(
									Arrays.copyOfRange(byteBuffer, 1+32, (1+32+1024))
									, StandardCharsets.UTF_8.displayName()));
							System.out.println("--- State changed from VALUE -> CMD");

							// reply to client that operation was successful
							out.write(outByteBuffer, 0, 1);

							// a complete command came from client. Time to expunge our local buffer
							// the following line creates another buffer of the same size BUFSIZE, but shifts
							// data by (1+1024+32) and pads the remaining bytes with null bytes
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
			}

			// // Receive until client closes connection, indicated by -1 return
			// while ((recvMsgSize = in.read(byteBuffer)) != -1)
			// out.write(byteBuffer, 0, recvMsgSize);

			System.out.println("Closing socket.");
			clntSock.close(); // Close the socket. We are done with this client!
		}
	}
}