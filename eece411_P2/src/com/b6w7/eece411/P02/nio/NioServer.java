package com.b6w7.eece411.P02.nio;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;
import com.b6w7.eece411.P02.multithreaded.Command;
import com.b6w7.eece411.P02.multithreaded.GetCommand;
import com.b6w7.eece411.P02.multithreaded.NodeCommands;
import com.b6w7.eece411.P02.multithreaded.PutCommand;
import com.b6w7.eece411.P02.multithreaded.RemoveCommand;
import com.b6w7.eece411.P02.multithreaded.UnrecognizedCommand;
import com.b6w7.eece411.P02.multithreaded.NodeCommands.Request;

public class NioServer extends Thread {

	private final long TIMEOUT_SELECTOR_MS = 5000;
	private final int SERVER_PORT;
	private final int NUM_NODES = 100;
	private final int SZ_BUFFER = 1024 * 4;

	private ServerSocketChannel server = null;
	private Selector selector = null;
	private SocketChannelBuffers buffers = new SocketChannelBuffers(NUM_NODES, SZ_BUFFER);
	private ByteBuffer buffer = null;
	private Request[] requests = NodeCommands.Request.values();


	//temp
	private Map<ByteArrayWrapper, byte[]> map;

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

				// Iterate over the keys, removing each key as we process the set 
				keys = selector.selectedKeys();
				i = keys.iterator();

				while(i.hasNext()) {
					// Remove the current key and process
					key = (SelectionKey) i.next();
					i.remove();

					if (key.isAcceptable()) {
						// This (server) socket channel is ready to accept a new (client) socket channel.
						// Get client socket channel, configure non-blocking, and register client as R+W.
						client = server.accept();
						client.configureBlocking(false);
						client.register(selector, SelectionKey.OP_READ);

					} else if (key.isReadable()) {
						// This socket channel is ready to read from pipe.  Get internal buffer for this
						// socket channel, and write bytes from pipe into buffer.  If buffer for this sc
						// has not been allocated yet (which is always the case for the first read) 
						// then we first allocate this buffer
						client = (SocketChannel) key.channel();
						bufferKey = (Integer) key.attachment();
						if (null == bufferKey)
							bufferKey = buffers.allocate();
						
						buffer = buffers.get(bufferKey);

						try {
							client.read(buffer);
						} catch (Exception e) {}
						
						Command cmd = parseCommand(buffer);

						client.register(selector, SelectionKey.OP_READ|SelectionKey.OP_WRITE, buffers.allocate());
					} else if (key.isWritable()) {
						// This socket channel is ready to write to pipe.  Get internal buffer for this
						// socket channel, and write bytes from buffer into pipe.
						client = (SocketChannel) key.channel();
						bufferKey = (Integer)key.attachment();
						client.write(buffers.get(bufferKey));

					} else if (key.isConnectable()) {
						// This (local) socket channel is ready to complete connection to (remote) socket.
						// Get local socket channel, and complete the connection.
						client = (SocketChannel) key.channel();
						client.finishConnect();
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


	private Command parseCommand(ByteBuffer b) {
		Command cmd;
		int cmdByte = (int)b.get();
		if (cmdByte < 0 || cmdByte > requests.length)
			cmdByte = Request.CMD_UNRECOG.getCode();
		
//		switch (requests[cmdByte]) {
//		case CMD_PUT:
//			cmd = new PutCommand(b, map);
//			//System.out.println("Issuing "+cmd);
//			db.post(cmd);
//			break;				
//		case CMD_GET:
//			cmd = new GetCommand(key, map);
//			//System.out.println("Issuing:  "+cmd);
//			db.post(cmd);
//			break;				
//		case CMD_REMOVE:
//			cmd = new RemoveCommand(key, map);
//			//System.out.println("Issuing:  "+cmd);
//			db.post(cmd);
//			break;		
//		default:
//			cmd = new UnrecognizedCommand();
//			//System.out.println("Issuing:  "+cmd);
//		}
			
		return null;
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