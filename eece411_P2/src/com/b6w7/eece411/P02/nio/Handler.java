package com.b6w7.eece411.P02.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Map;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;
import com.b6w7.eece411.P02.multithreaded.Command;
import com.b6w7.eece411.P02.multithreaded.NodeCommands;
import com.b6w7.eece411.P02.multithreaded.NodeCommands.Reply;
import com.b6w7.eece411.P02.multithreaded.NodeCommands.Request;
import com.b6w7.eece411.P02.multithreaded.PostCommand;

final class Handler extends Command implements Runnable { 
	private final SocketChannel socketRequester;
	private final SelectionKey keyRequester;
	private ByteBuffer input = ByteBuffer.allocate(2048);
	private ByteBuffer output;
	private SocketChannel socketOwner;
	private SelectionKey keyOwner;

	private static final int CMDSIZE = NodeCommands.LEN_CMD_BYTES;		
	private static final int KEYSIZE = NodeCommands.LEN_KEY_BYTES;
	private static final int VALUESIZE = NodeCommands.LEN_VALUE_BYTES;

	byte cmd = Request.CMD_NOT_SET.getCode();
	byte[] key;
	byte[] value;
	byte replyCode = Reply.CMD_NOT_SET.getCode();
	byte[] replyValue;

	private final PostCommand dbHandler;

	/**
	 * The state of this {@link Command} used as a FSM
	 */
	protected State state = State.RECV_REQUESTER;

	// frequently used, so stored here for future use
	private Request[] requests = Request.values();
	private Selector sel;
	private Process process;

	// possible states of any command
	enum State {
		RECV_REQUESTER,
		CHECKING_LOCAL,
		SEND_OWNER,
		RECV_OWNER,
		SEND_REQUESTER,
	}


	Handler(Selector sel, SocketChannel c, PostCommand dbHandler, Map<ByteArrayWrapper, byte[]> map) 
			throws IOException {

		this.sel = sel;

		if (null == map) 
			throw new IllegalArgumentException("map cannot be null");

		this.dbHandler = dbHandler;
		this.map = map;
		socketRequester = c; 
		c.configureBlocking(false);
		// Optionally try first read now
		keyRequester = socketRequester.register(sel, 0);
		keyRequester.attach(this);
		keyRequester.interestOps(SelectionKey.OP_READ);
		sel.wakeup();
	}

	boolean inputIsComplete() {
		int cmdInt;

		// if the position is at 0 then we have nothing to read
		// may not be a needed check since we are non-blocking I/O
		if (input.position() < CMDSIZE)
			return false;

		// We need to know what operation this is
		if (Request.CMD_NOT_SET.getCode() == cmd) {
			cmd = input.get(0);
		}

		// Now we know what operation, now we need to know how many bytes that we expect
		cmdInt = (int)cmd;
		if (cmdInt >= requests.length || cmdInt < 0 )
			cmd = Request.CMD_UNRECOG.getCode();

		switch (requests[cmd]) {
		case CMD_GET:
			if (input.position() >= CMDSIZE + KEYSIZE) {
				process = new GetProcess();
				return true;
			}
			return false;

		case CMD_PUT:
			if (input.position() >= CMDSIZE + KEYSIZE + VALUESIZE) {
				process = new PutProcess();
				return true;
			}
			return false;

		case CMD_REMOVE:
			if (input.position() >= CMDSIZE + KEYSIZE) {
				process = new RemoveProcess();
				return true;
			}
			return false;

		case CMD_NOT_SET:
		case CMD_UNRECOG:
		default:
			process = new UnrecogProcess();
			// bad command received on wire
			// nothing to do
			return true;
		}
	}

	boolean outputIsComplete() {
		return !output.hasRemaining();
	}

	void processRecvRequester() {
		dbHandler.post(this);
	}

	@Override
	public void run() {
		try { 
			switch (state) {
			case RECV_REQUESTER:
				if (IS_VERBOSE) System.out.println(" --- run(): RECV_REQUESTER");
				recvRequester();
				break;

			case CHECKING_LOCAL:
				throw new IllegalStateException("CHECKING_LOCAL should not be called in run()");

			case SEND_OWNER:
				if (IS_VERBOSE) System.out.println(" --- run(): SEND_OWNER");
				sendOwner();
				break;

			case RECV_OWNER:
				if (IS_VERBOSE) System.out.println(" --- run(): RECV_OWNER");
				recvOwner();
				break;

			case SEND_REQUESTER:
				if (IS_VERBOSE) System.out.println(" --- run(): SEND_REQUESTER");
				sendRequester();
			}
		} catch (IOException ex) { /* ... */ }
	} 

	// only call this method if we have not received enough bytes for a complete
	// operation.  This can be called multiple times until enough bytes are received.
	private void recvRequester() throws IOException {
		// read from the socket
		socketRequester.read(input);

		if (inputIsComplete()) {
			// TODO shortcircuit FSM by checking Hash(key) to know if it is locally
			// stored or remotely stored.  For now, always check local first.
			state = State.CHECKING_LOCAL;
			keyRequester.interestOps(0);
			processRecvRequester(); 
		}
	}

	private void sendOwner() {
		throw new NotImplementedException();
	}

	private void recvOwner() {
		throw new NotImplementedException();
	}

	private void sendRequester() throws IOException {
		output.flip();
		socketRequester.write(output);

		if (outputIsComplete())
			keyRequester.cancel();
	}

	class PutProcess implements Process {

		@Override
		public void process() {
			if( put() )
				replyCode = Reply.RPY_SUCCESS.getCode(); 
			else
				replyCode = Reply.RPY_OUT_OF_SPACE.getCode();

			generateReply();

			state = State.SEND_REQUESTER;
			keyRequester.interestOps(SelectionKey.OP_WRITE);
			sel.wakeup();
		}

		@Override
		public void generateReply() {
			output = ByteBuffer.allocate( NodeCommands.LEN_CMD_BYTES );
			output.put(replyCode);
		}

		private boolean put(){
			//		System.out.println(" --- put(): input.position()==" + input.position());
			//		System.out.println(" --- put(): input.limit()==" + input.limit());
			//		System.out.println(" --- put(): input.capacity()==" + input.capacity());
			key = new byte[KEYSIZE];
			key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);

			if(map.size() == MAX_MEMORY && map.containsKey(key) == false ){
				//System.out.println("reached MAX MEMORY "+MAX_MEMORY+" with: ("+k.toString()+", "+s.toString()+")");
				//replyCode = NodeCommands.RPY_OUT_OF_SPACE;
				return false;

			} else {
				value = new byte[VALUESIZE];
				value = Arrays.copyOfRange(input.array(), CMDSIZE+KEYSIZE, CMDSIZE+KEYSIZE+VALUESIZE);
				byte[] result = map.put(new ByteArrayWrapper(key), value);

				if(result != null) {
					// Overwriting -- we take note
					System.out.println("*** PutCommand() Replacing Key " + this.toString());
				}

				return true;
			}
		}
	}
	
	class GetProcess implements Process {

		@Override
		public void process() {
			replyValue = get();
			if( replyValue != null )  
				replyCode = Reply.RPY_SUCCESS.getCode(); 
			else
				replyCode = Reply.RPY_INEXISTENT.getCode();

			generateReply();

			state = State.SEND_REQUESTER;
			keyRequester.interestOps(SelectionKey.OP_WRITE);
			sel.wakeup();
		}

		@Override
		public void generateReply() {
			if(replyValue != null){
				output = ByteBuffer.allocate( NodeCommands.LEN_CMD_BYTES + NodeCommands.LEN_VALUE_BYTES);
				output.put(replyCode);
				output.put(replyValue);
			} else {
				output = ByteBuffer.allocate( NodeCommands.LEN_CMD_BYTES );
				output.put(replyCode);
			}
		}

		private byte[] get(){
			key = new byte[KEYSIZE];
			key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);
			byte[] val = map.get( new ByteArrayWrapper(key) );

			//		System.out.println("(key.length, get key bytes): ("+key.length+
			//				", "+NodeCommands.byteArrayAsString(key) +")" );
			if(val == null) {
				// NONEXISTENT -- we want to debug here
				if (IS_VERBOSE) System.out.println("*** GetCommand() Not Found " + this.toString());
			}
			return val;
		}
	}

	class RemoveProcess implements Process {
	
		@Override
		public void process() {
			replyValue = remove();
			if( replyValue != null ) 
				replyCode = Reply.RPY_SUCCESS.getCode(); 
			else
				replyCode = Reply.RPY_INEXISTENT.getCode();
	
			generateReply();
	
			state = State.SEND_REQUESTER;
			keyRequester.interestOps(SelectionKey.OP_WRITE);
			sel.wakeup();
		}
	
		@Override
		public void generateReply() {
			if(replyValue != null){
				output = ByteBuffer.allocate( NodeCommands.LEN_CMD_BYTES + NodeCommands.LEN_VALUE_BYTES);
				output.put(replyCode);
				output.put(replyValue);
			} else {
				output = ByteBuffer.allocate( NodeCommands.LEN_CMD_BYTES );
				output.put(replyCode);
			}
		}
		
		private byte[] remove(){
			//		System.out.println("(key.length, get key bytes): ("+key.length+
			//				", "+NodeCommands.byteArrayAsString(key) +")" );
			key = new byte[KEYSIZE];
			key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);
			return map.remove(new ByteArrayWrapper(key));
		}
	
	}
	
	class UnrecogProcess implements Process {

		@Override
		public void process() {
			replyCode = NodeCommands.Reply.CMD_UNRECOGNIZED.getCode();
			generateReply();
			
			state = State.SEND_REQUESTER;
			keyRequester.interestOps(SelectionKey.OP_WRITE);
			sel.wakeup();
		}
		

		@Override
		public void generateReply() {
			output = ByteBuffer.allocate( NodeCommands.LEN_CMD_BYTES );
			output.put(replyCode);
		}
		
	}

	@Override
	public void execute() {
		switch (state) {
		case RECV_REQUESTER:
			throw new IllegalStateException("RECV_REQUESTER should not be called in execute()");

		case CHECKING_LOCAL:
			if (IS_VERBOSE) System.out.println(" --- execute(): CHECKING_LOCAL");
			process.process();
			break;

		case SEND_OWNER:
			throw new IllegalStateException("SEND_OWNER should not be called in execute()");

		case RECV_OWNER:
			throw new IllegalStateException("RECV_OWNER should not be called in execute()");

		case SEND_REQUESTER:
			throw new IllegalStateException("SEND_REQUESTER should not be called in execute()");
		}
	}

	private boolean put(){
		//		System.out.println(" --- put(): input.position()==" + input.position());
		//		System.out.println(" --- put(): input.limit()==" + input.limit());
		//		System.out.println(" --- put(): input.capacity()==" + input.capacity());
		key = new byte[KEYSIZE];
		key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);

		if(map.size() == MAX_MEMORY && map.containsKey(key) == false ){
			//System.out.println("reached MAX MEMORY "+MAX_MEMORY+" with: ("+k.toString()+", "+s.toString()+")");
			//replyCode = NodeCommands.RPY_OUT_OF_SPACE;
			return false;

		} else {
			value = new byte[VALUESIZE];
			value = Arrays.copyOfRange(input.array(), CMDSIZE+KEYSIZE, CMDSIZE+KEYSIZE+VALUESIZE);
			byte[] result = map.put(new ByteArrayWrapper(key), value);

			if(result != null) {
				// Overwriting -- we take note
				System.out.println("*** PutCommand() Replacing Key " + this.toString());
			}

			return true;
		}
	}



	@Override
	public byte[] getReply() {
		throw new UnsupportedOperationException("To be removed from Commmand interface");
	}

	@Override
	public String toString(){

		StringBuilder s = new StringBuilder();

		s.append("[command=>");
		s.append(requests[(int)cmd]);
		s.append("] [key=>");
		if (null != key) {
			for (int i=0; i<LEN_TO_STRING_OF_KEY; i++)
				s.append(Integer.toString((key[i] & 0xff) + 0x100, 16).substring(1));
		} else {
			s.append("null");
		}
		if (null != value) {
			s.append("] [value["+value.length+"]=>");
			for (int i=0; i<LEN_TO_STRING_OF_VAL; i++)
				s.append(Integer.toString((value[i] & 0xff) + 0x100, 16).substring(1));
		} else {
			s.append("] [value[-]=>null");
		}

		s.append("] [replyCode=>");
		s.append(NodeCommands.Reply.values()[replyCode].toString());
		s.append("]");

		return s.toString();
	}

}