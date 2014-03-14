package com.b6w7.eece411.P02.nio;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class NioServer extends Thread {

	private final long TIMEOUT_SELECTOR_MS = 5000;
	private final int SERVER_PORT;
	private final int NUM_NODES = 100;
	private final int SZ_BUFFER = 1024 * 4;

	private ServerSocketChannel server = null;
	private Selector selector = null;
	private SocketChannelBuffers buffers = new SocketChannelBuffers(NUM_NODES, SZ_BUFFER);
	

	public NioServer(int servPort) {
		SERVER_PORT = servPort;
	}

	@Override
	public void run() {
		try {
			Set<SelectionKey> keys = null;
			Iterator<SelectionKey> i = null;
			SelectionKey key = null;
			SocketChannel client = null;
			Integer bufferKey = null;

			// Create the server socket channel
			server = ServerSocketChannel.open();

			// nonblocking I/O
			server.configureBlocking(false);

			// bind to SERVER_PORT
			server.socket().bind(new InetSocketAddress(InetAddress.getLocalHost(), SERVER_PORT));

			// Create the selector
			selector = Selector.open();

			// Recording server to selector (type OP_ACCEPT)
			server.register(selector, SelectionKey.OP_ACCEPT);
			
			SocketChannel socketChannel = SocketChannel.open();
			socketChannel.configureBlocking(false);
			socketChannel.connect(new InetSocketAddress(InetAddress.getLocalHost(), SERVER_PORT));
			while (!socketChannel.isConnected())
				socketChannel.finishConnect();
			
			server.register(selector, SelectionKey.OP_CONNECT);
			
			for(;;) {
				// Waiting for events
				selector.select(TIMEOUT_SELECTOR_MS);

				// Get the keys
				keys = selector.selectedKeys();
				i = keys.iterator();

				// For each keys...
				while(i.hasNext()) {
					// Remove the current key and process
					key = (SelectionKey) i.next();
					i.remove();

					if (key.isAcceptable()) {
						// This (server) socket channel is ready to accept a new (client) socket channel.
						// Get client socket channel, configure non-blocking, and register client as readable.
						client = server.accept();
						client.configureBlocking(false);
						client.register(selector, SelectionKey.OP_READ, buffers.allocate());

					} else if (key.isReadable()) {
						// This socket channel is ready to read from pipe.  Get internal buffer for this
						// socket channel, and write bytes from pipe into buffer.
						bufferKey = (Integer) key.attachment();
						client = (SocketChannel) key.channel();
						try {
							client.read(buffers.get(bufferKey));
						} catch (Exception e) {}
						
					} else if (key.isConnectable()) {
						// This (local) socket channel is ready to complete connection to (remote) socket.
						// Get local socket channel, and complete the connection.
						client = (SocketChannel) key.channel();
						client.finishConnect();
						
					} else if (key.isWritable()) {
						// This socket channel is ready to write to pipe.  Get internal buffer for this
						// socket channel, and write bytes from buffer into pipe.
						client = (SocketChannel) key.channel();
						bufferKey = (Integer)key.attachment();
						client.write(buffers.get(bufferKey));
					}
					
				}  // end while (i.hasNext())
			}      // end for(;;)

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		} finally {
			if (null != server) {
				try {
					server.close();
				} catch (IOException e) {}
			}

		}
	}

	private static void printUsage() {
		System.out.println("USAGE:\n"
				+ " java -cp"
				+ " <file.jar>"
				+ " <server port>");
		System.out.println("EXAMPLE:\n"
				+ " java -cp"
				+ " P03.jar"
				+ " 11111");
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			printUsage();
			return;
		}

		int servPort = Integer.parseInt(args[0]);
		NioServer server = new NioServer(servPort);
		server.start();
	}
}