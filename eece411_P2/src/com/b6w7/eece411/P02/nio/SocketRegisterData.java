package com.b6w7.eece411.P02.nio;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class SocketRegisterData {
	SelectionKey key;
	final SocketChannel sc;
	final int ops;
	final Runnable cmd;
	final InetSocketAddress addr;
	SocketRegisterData(SelectionKey key, SocketChannel channel, int operations, Runnable cmd, InetSocketAddress addr) {
		this.key = key;
		this.sc = channel;
		this.ops = operations;
		this.cmd = cmd;
		this.addr = addr;
	}
}
