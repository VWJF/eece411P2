package com.b6w7.eece411.P02.nio;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.Queue;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;
import com.b6w7.eece411.P02.multithreaded.Command;
import com.b6w7.eece411.P02.multithreaded.NodeCommands;
import com.b6w7.eece411.P02.multithreaded.NodeCommands.Reply;
import com.b6w7.eece411.P02.multithreaded.NodeCommands.Request;
import com.b6w7.eece411.P02.multithreaded.PostCommand;
import com.b6w7.eece411.P02.nio.ConsistentHashing.Membership;

final class Handler extends Command implements Runnable { 
	private final SocketChannel socketRequester;
	private final SelectionKey keyRequester;
	private ByteBuffer input = ByteBuffer.allocate(2048);
	private ByteBuffer output;
	private SocketChannel socketOwner;
	private SelectionKey keyOwner;

	private static final int CMDSIZE = NodeCommands.LEN_CMD_BYTES;		
	private static final int RPYSIZE = NodeCommands.LEN_CMD_BYTES;		
	private static final int KEYSIZE = NodeCommands.LEN_KEY_BYTES;
	private static final int VALUESIZE = NodeCommands.LEN_VALUE_BYTES;
	private static final int TSVECTOR = NodeCommands.LEN_TIMESTAMP_BYTES;

	byte cmd = Request.CMD_NOT_SET.getCode();
	byte[] key;
	ByteArrayWrapper hashedKey;
	byte[] value;
	byte replyCode = Reply.RPY_NOT_SET.getCode();
	byte[] replyValue;

	private final MembershipProtocol membership;
	private final PostCommand dbHandler;

	/**
	 * The state of this {@link Command} used as a FSM
	 */
	protected State state = State.RECV_REQUESTER;

	// frequently used, so stored here for future use
	private final Request[] requests = Request.values();
	private final Reply[] replies = Reply.values();
	private Selector sel;
	private Process process;
	private Queue<SocketRegisterData> queue;
	private SocketRegisterData remote;
	
	private boolean mergeComplete = false;

	
	// debug 
	private boolean useRemote;

	// possible states of any command
	enum State {
		RECV_REQUESTER,
		CHECKING_LOCAL,
		CONNECT_OWNER,
		SEND_OWNER,
		RECV_OWNER,
		SEND_REQUESTER, 
		ABORT, 
	}


	Handler(Selector sel, SocketChannel c, PostCommand dbHandler, ConsistentHashing<ByteArrayWrapper, byte[]> map, Queue<SocketRegisterData> queue, boolean useRemote) 
			throws IOException {

		
		this.queue = queue;
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
		
		String localhost = InetAddress.getLocalHost().getCanonicalHostName();
		membership = new MembershipProtocol(map.getNodePosition(localhost), map.getSizeAllNodes());
		this.mergeComplete = false;
		// debug
		this.useRemote = useRemote;
	}

	/**
	 * checks if buffer has been completely written.  Has same affect as 
	 * testing output.position == output.limit
	 * @return true if buffer has been completely written; false otherwise.
	 */
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
				if (IS_VERBOSE) System.out.println(" --- run(): RECV_REQUESTER " +this);
				recvRequester();
				break;

			case CHECKING_LOCAL:
				throw new IllegalStateException("CHECKING_LOCAL should not be called in run()");

			case CONNECT_OWNER:
				if (IS_VERBOSE) System.out.println(" --- run(): CONNECT_OWNER " +this);
				connectOwner();
				break;

			case SEND_OWNER:
				if (IS_VERBOSE) System.out.println(" --- run(): SEND_OWNER " +this);
				sendOwner();
				break;

			case RECV_OWNER:
				if (IS_VERBOSE) System.out.println(" --- run(): RECV_OWNER " +this);
				recvOwner();
				break;

			case SEND_REQUESTER:
				if (IS_VERBOSE) System.out.println(" --- run(): SEND_REQUESTER " +this);
				sendRequester();
				break;
				
			case ABORT:
				// TODO fill in
				break;
			}
		} catch (IOException ex) { /* ... */ }
	} 
	
	@Override
	public void execute() {
		switch (state) {
		case RECV_REQUESTER:
			throw new IllegalStateException("RECV_REQUESTER should not be called in execute()");

		case CHECKING_LOCAL:
			if (IS_VERBOSE) System.out.println(" --- execute(): CHECKING_LOCAL " + this);
			//TODO:membership.
			process.checkLocal();
			break;

		case CONNECT_OWNER:
			throw new IllegalStateException("CONNECT_OWNER should not be called in execute()");

		case SEND_OWNER:
			throw new IllegalStateException("SEND_OWNER should not be called in execute()");

		case RECV_OWNER:
			throw new IllegalStateException("RECV_OWNER should not be called in execute()");

		case SEND_REQUESTER:
			throw new IllegalStateException("SEND_REQUESTER should not be called in execute()");
		default:
			break;
		}
	}


	// only call this method if we have not received enough bytes for a complete
	// operation.  This can be called multiple times until enough bytes are received.
	private void recvRequester() throws IOException {
		// read from the socket
		//if (IS_VERBOSE) System.out.println(" +++ PutProcess::recvRequester() BEFORE input.position()=="+input.position()+" input.limit()=="+input.limit());
		socketRequester.read(input);
		//if (IS_VERBOSE) System.out.println(" +++ PutProcess::recvRequester() AFTER input.position()=="+input.position()+" input.limit()=="+input.limit());

		if (requesterInputIsComplete()) {
			if (IS_VERBOSE) System.out.println(" +++ PutProcess::recvRequester() COMPLETE input.position()=="+input.position()+" input.limit()=="+input.limit());
			state = State.CHECKING_LOCAL;
			keyRequester.interestOps(0);
			processRecvRequester(); 
		}
	}
	
	// returns true if enough was received from requester to go to next state CHECK_LOCAL
	// side effect: 
	// sets cmd appropriately, 
	// sets process to the corresponding Process subtype
	private boolean requesterInputIsComplete() {
		int position = input.position();
		cmd = input.get(0);
		cmd = NodeCommands.sanitizeCmd(cmd);

		// Now we know what operation, now we need to know how many bytes that we expect
		if (Request.CMD_GET.getCode() == cmd){
			if (position >= CMDSIZE + KEYSIZE) {
				process = new GetProcess();
				input.position(CMDSIZE + KEYSIZE);
				input.flip();
				return true;
			}
			return false;
			
		} else if (Request.CMD_PUT.getCode() == cmd) {
			if (position >= CMDSIZE + KEYSIZE + VALUESIZE) {
				process = new PutProcess();
				input.position(CMDSIZE + KEYSIZE + VALUESIZE);
				input.flip();
				return true;
			}
			return false;
			
		} else if (Request.CMD_REMOVE.getCode() == cmd) {
			if (position >= CMDSIZE + KEYSIZE) {
				process = new RemoveProcess();
				input.position(CMDSIZE + KEYSIZE);
				input.flip();
				return true;
			}
			return false;

		} else if (Request.CMD_TS_GET.getCode() == cmd) {
			if (position >= CMDSIZE + KEYSIZE + TSVECTOR);
			process = new TSGetProcess();
			input.position(CMDSIZE + KEYSIZE + TSVECTOR);
			input.flip();
			return true;
			
		} else {
			process = new UnrecogProcess();
			// bad command received on wire
			// nothing to do
			input.position(CMDSIZE);
			input.flip();
			return true;
		}
	}


	/**
	 * Common method for all commands to complete their connection to a remote node
	 * We obtain the SelectionKey to the remote because we saved a reference
	 * to SocketRegisterData before passing it to Selector thread to connect.  When 
	 * selector thread performed .register(). it saved the key in the object.  We can
	 * now safely access that reference to obtain the SelectionKey to the remote.
	 * side-effect:
	 * if socketOwner is ready to connect, completes connection,
	 * transitions to SEND_OWNER, and enables the SelectionKey for writing
	 * If the Host is unknown, then transition to ABORT 
	 */
	private void connectOwner() {
		try {
			if(IS_SHORT) System.out.println("Waiting for connectOwner to complete");
			try {
				if (socketOwner.finishConnect())  { // {System.out.print("a");}
					keyOwner = remote.key;
					if (keyOwner == null) {
						if(IS_SHORT) System.out.println("*** key is null");
						sel.wakeup();

					} else {
						state = State.SEND_OWNER;
						keyOwner.interestOps(SelectionKey.OP_WRITE);
						sel.wakeup();
					}
				}

			} catch (ConnectException e1) {
				// We could not contact the owner node, so reply that internal error occurred
				replyCode = Reply.RPY_INTERNAL_FAILURE.getCode();
				process.generateRequesterReply();
				state = State.SEND_REQUESTER;
				keyRequester.interestOps(SelectionKey.OP_WRITE);
			}

		} catch (UnknownHostException e) {
			abort(e);

		} catch (IOException e) {
			retryAtStateCheckingLocal(e);
		}
	}

	/**
	 * Common method for delivering contents of output to remote node
	 * side-effect:
	 * output.position will be incremented by as many bytes that can be written
	 * up to output.limit.  If not all bytes are written, this method may be 
	 * called again repeatedly until output.position = output.limit.
	 * When this happens, transitions to state RECV_OWNER and set interestOps
	 * to ready to read.  Also resets output.position to 0 for RECV_OWNER.
	 * If network error occurs, then deallocate all network resources and
	 * retry at CHECKING_LOCAL state.
	 */
	private void sendOwner() {
		try {
			socketOwner.write(output);
		} catch (IOException e) {
			retryAtStateCheckingLocal(e);
		}

		if (outputIsComplete()) {
			state = State.RECV_OWNER;
			keyOwner.interestOps(SelectionKey.OP_READ);
			output.position(0); // need to set to zero before entering RECV_OWNER!
			sel.wakeup();
		}
	}

	private void recvOwner() {
		process.recvOwner();
	}

	/**
	 * Common method for all commands to write a reply to requester.
	 * Note that we use input (not output) buffer.  see {@link Process}
	 * for the reasons.  This may be called repeatedly until all bytes
	 * are written.  If the reply is completely written then deallocate
	 * all network resources and close connection.
	 * side-effects:
	 * input.position is incremented some amount up to N.
	 * @throws IOException
	 */
	private void sendRequester() {
		try {
			if (null == socketRequester || !socketRequester.isOpen()) {
				System.out.println("### sendRequest() socketRequester is still non-null or open");
				return;
			}

			if (IS_VERBOSE) System.out.println(" +++ Common::sendRequester() BEFORE output.position()=="+output.position()+" output.limit()=="+output.limit());
			socketRequester.write(output);
			if (IS_VERBOSE) System.out.println(" +++ Common::sendRequester() AFTER output.position()=="+output.position()+" output.limit()=="+output.limit());

			if (outputIsComplete()) {
				if (IS_VERBOSE) System.out.println(" +++ Common::sendRequester() COMPLETED output.position()=="+output.position()+" output.limit()=="+output.limit());
				if (null != keyRequester && keyRequester.isValid()) { keyRequester.cancel(); }
				if (null != keyOwner && keyOwner.isValid()) { keyOwner.cancel(); }
				if (null != socketRequester) socketRequester.close();
				if (null != socketOwner) socketOwner.close();
			}
		} catch (IOException e) {
			// dont retry
			// retryAtStateCheckingLocal(e);
		} finally {
			if (null != keyRequester && keyRequester.isValid()) { keyRequester.cancel(); }
			if (null != keyOwner && keyOwner.isValid()) { keyOwner.cancel(); }
		}
	}
	
	/**
	 * Unrecoverable network error occurred.  Deallocate all network resources and
	 * abort by transitioning to state ABORT
	 * @param e Exception that occurred
	 */
	private void abort(Exception e) {
		// Unknown host.  Fatal error.  Abort this command.
		System.out.println("*** Unknown Host. Aborting. "+e.getMessage());
		try {
			if (null != socketOwner) socketOwner.close();
		} catch (IOException e1) {}
		if (null != keyOwner) keyOwner.cancel();
		state = State.ABORT;
	}

	/**
	 * Network error occurred.  Deallocate all network resources with remote and
	 * try again from state CHECK_LOCAL.  Also remove interestOps with requester.
	 * The first step in that state
	 * will test locally for key, then immediately transition to CONNECT_OWNER.
	 * input.position is reset to 0 to erase anything we might have read
	 * into it during RECV_OWNER.
	 * @param e Exception that occurred
	 */
	private void retryAtStateCheckingLocal(Exception e) {
		// TODO add a retry counter or time to fail?
		System.out.println("*** Network error in connecting to remote node. "+ e.getMessage());
		e.printStackTrace();
		try {
			if (null != socketOwner) socketOwner.close();
		} catch (IOException e2) {}
		if (null != keyOwner) keyOwner.cancel();
		if (null != keyRequester) keyRequester.interestOps(0);
		state = State.CHECKING_LOCAL;
		input.position(0);
		processRecvRequester();
	}

	class PutProcess implements Process {
		@Override
		public void checkLocal() {
			if (IS_VERBOSE) System.out.println(" --- PutProcess::checkLocal(): " + this);

			key = new byte[KEYSIZE];
			key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);
			value = new byte[VALUESIZE];
			value = Arrays.copyOfRange(input.array(), CMDSIZE+KEYSIZE, CMDSIZE+KEYSIZE+VALUESIZE);
			hashedKey = new ByteArrayWrapper(key);
			output = ByteBuffer.allocate(2048);
			
			if (useRemote) {
				// OK, we decided that the location of key is at a remote node
				// we can transition to CONNECT_OWNER and connect to remote node
				if(IS_SHORT) System.out.println("--- checkLocal() Using remote");
				state = State.CONNECT_OWNER;

				try {
					// prepare the output buffer, and signal for opening a socket to remote
					generateOwnerQuery();

					socketOwner = SocketChannel.open();
					socketOwner.configureBlocking(false);
					// Send message to selector and wake up selector to process the message.
					// There is no need to set interestOps() because selector will check its queue.
					registerData(keyOwner, socketOwner, SelectionKey.OP_CONNECT);
					sel.wakeup();

				} catch (IOException e) {
					retryAtStateCheckingLocal(e);
				}

			} else {
				// OK, we decided that the location of key is at local node
				// perform appropriate action with database
				// we can transition to SEND_REQUESTER
				
				// set replyCode as appropriate and prepare output buffer
				if(IS_SHORT) System.out.println("--- checkLocal() Using Local");
				if( put() )
					replyCode = Reply.RPY_SUCCESS.getCode(); 
				else
					replyCode = Reply.RPY_OUT_OF_SPACE.getCode();

				generateRequesterReply();

				// signal to selector that we are ready to write
				state = State.SEND_REQUESTER;
				keyRequester.interestOps(SelectionKey.OP_WRITE);
				sel.wakeup();
			}
		}

		@Override
		public void generateRequesterReply() {
			if (output.position() != 0) {
//				if (output.position() != RPYSIZE)
//					System.out.println("### PutProcess::output.position()=="+output.position()+ " != 1");
				// This means that input received the contents of a
				// RECV_OWNER and can be directly forwarded to requester
				output.flip();
				if (IS_VERBOSE) System.out.println(" +++ PutProcess::generateRequesterReply() COMPLETED CASE ONE output.position()=="+output.position()+" output.limit()=="+output.limit());
				return;
			}
			
			// We performed a local look up.  So we fill in input with 
			// the appropriate reply to requester.
			output.put(replyCode);
			output.flip();
			if (IS_VERBOSE) System.out.println(" +++ PutProcess::generateRequesterReply() COMPLETED CASE TWO output.position()=="+output.position()+" output.limit()=="+output.limit());
		}

		@Override
		public void generateOwnerQuery() {
			output.position(0);
			output.put(cmd);
			output.put(key);
			output.put(value);
			output.flip();
		}

		@Override
		public void recvOwner() {
			// read from the socket
			try {
//				if (IS_VERBOSE) System.out.println(" +++ PutProcess::recvOwner() BEFORE output.position()=="+output.position()+" output.limit()=="+output.limit());
				socketOwner.read(output);
//				if (IS_VERBOSE) System.out.println(" +++ PutProcess::recvOwner() AFTER output.position()=="+output.position()+" output.limit()=="+output.limit());

				if (recvOwnerIsComplete()) {
					if (IS_VERBOSE) System.out.println(" +++ PutProcess::recvOwner() COMPLETE output.position()=="+output.position()+" output.limit()=="+output.limit());
					generateRequesterReply();
					
					state = State.SEND_REQUESTER;
					keyRequester.interestOps(SelectionKey.OP_WRITE);
					sel.wakeup();
				}

			} catch (IOException e) {
				retryAtStateCheckingLocal(e);
			}
		}


		/**
		 * Checks that enough bytes are input to create a valid reply from owner.
		 * If so, assign output to input.  This saves a buffer copy when we forward
		 * the owner's reply to the requester.
		 * side effects:
		 * Both output and input will reference what was previously input
		 * position set to 0, limit set to N
		 * replyCode is set accordingly.
		 * @return true if enough bytes read; false otherwise.
		 */
		private boolean recvOwnerIsComplete() {
			replyCode = output.get(0);
			output.position(RPYSIZE);
			output.flip();
			
			return true;
		}

		private boolean put(){
			//		System.out.println(" --- put(): input.position()==" + input.position());
			//		System.out.println(" --- put(): input.limit()==" + input.limit());
			//		System.out.println(" --- put(): input.capacity()==" + input.capacity());

			if(map.size() == MAX_MEMORY && map.containsKey(hashedKey) == false ){
				//System.out.println("reached MAX MEMORY "+MAX_MEMORY+" with: ("+k.toString()+", "+s.toString()+")");
				//replyCode = NodeCommands.RPY_OUT_OF_SPACE;
				return false;

			} else {
				value = new byte[VALUESIZE];
				value = Arrays.copyOfRange(input.array(), CMDSIZE+KEYSIZE, CMDSIZE+KEYSIZE+VALUESIZE);
				byte[] result = map.put(hashedKey, value);

				if(result != null) {
					// Overwriting -- we take note
					System.out.println("*** PutCommand() Replacing Key " + this.toString());
				}

				return true;
			}
		}
	}
	
	class TSGetProcess extends GetProcess {
	}

	class GetProcess implements Process {

		@Override
		public void checkLocal() {
			if (IS_VERBOSE) System.out.println(" --- GetProcess::checkLocal(): " + this);
			
			key = new byte[KEYSIZE];
			key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);
			hashedKey = new ByteArrayWrapper(key);
			output = ByteBuffer.allocate(2048);
			
			//TODO: Steps can be moved to execute() state: CHECKING_LOCAL ??
			// Seems like they cannot be moves since the state:CHECKING_LOCAL may occur several times.
			if(mergeComplete == false){
				//TODO: mergeVector();
				membership.mergeVector(receivedVector);
				mergeComplete = true;
			}
			
			if (useRemote) {
				// OK, we decided that the location of key is at a remote node
				// we can transition to CONNECT_OWNER and connect to remote node
				System.out.println("--- GetProcess::checkLocal() Using remote");
				state = State.CONNECT_OWNER;

				try {
					// prepare the output buffer, and signal for opening a socket to remote
					generateOwnerQuery();

					socketOwner = SocketChannel.open();
					socketOwner.configureBlocking(false);
					// Send message to selector and wake up selector to process the message.
					// There is no need to set interestOps() because selector will check its queue.
					registerData(keyOwner, socketOwner, SelectionKey.OP_CONNECT);
					sel.wakeup();

				} catch (IOException e) {
					retryAtStateCheckingLocal(e);
				}

			} else {
				// OK, we decided that the location of key is at local node
				// perform appropriate action with database
				// we can transition to SEND_REQUESTER
				
				// set replyCode as appropriate and prepare output buffer
				replyValue = get();
				if( replyValue != null )  
					replyCode = Reply.RPY_SUCCESS.getCode(); 
				else
					replyCode = Reply.RPY_INEXISTENT.getCode();

				generateRequesterReply();

				state = State.SEND_REQUESTER;
				keyRequester.interestOps(SelectionKey.OP_WRITE);
				sel.wakeup();
			}
		}

		@Override
		public void generateRequesterReply() {
			if (output.position() != 0) {
				// This means that input received the contents of a
				// RECV_OWNER and can be directly forwarded to requester
				output.flip();
				if (IS_VERBOSE) System.out.println(" +++ GetProcess::generateRequesterReply() COMPLETED CASE ONE output.position()=="+output.position()+" output.limit()=="+output.limit());
				return;
			}

			// We performed a local look up.  So we fill in input with 
			// the appropriate reply to requester.
			output.put(replyCode);
			if (replyValue != null)
				output.put(replyValue);
			output.flip();
			if (IS_VERBOSE) System.out.println(" +++ GetProcess::generateRequesterReply() COMPLETED CASE TWO output.position()=="+output.position()+" output.limit()=="+output.limit());
		}

		@Override
		public void generateOwnerQuery() {
			output.position(0);
			output.put(cmd);
			output.put(key);
			output.flip();
		}

		@Override
		public void recvOwner() {
			// read from the socket
			try {
//				if (IS_VERBOSE) System.out.println(" +++ GetProcess::recvOwner() BEFORE output.position()=="+output.position()+" output.limit()=="+output.limit());
				socketOwner.read(output);
//				if (IS_VERBOSE) System.out.println(" +++ GetProcess::recvOwner() AFTER output.position()=="+output.position()+" output.limit()=="+output.limit());

				if (recvOwnerIsComplete()) {
					if (IS_VERBOSE) System.out.println(" +++ GetProcess::recvOwner() COMPLETED output.position()=="+output.position()+" output.limit()=="+output.limit());
					generateRequesterReply();
					
					state = State.SEND_REQUESTER;
					keyRequester.interestOps(SelectionKey.OP_WRITE);
					sel.wakeup();
				}

			} catch (IOException e) {
				retryAtStateCheckingLocal(e);
			}
		}

		private boolean recvOwnerIsComplete() {
			replyCode = output.get(0);
			
			if (Reply.RPY_SUCCESS.getCode() == replyCode) {
				if (output.position() >= RPYSIZE + VALUESIZE) {
					output.position(RPYSIZE + VALUESIZE);
					output.flip();
					return true;
				}
			}

			output.position(RPYSIZE);
			output.flip();
			return true;
		}

		private byte[] get(){
			byte[] val = map.get( hashedKey );
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
		public void checkLocal() {
			if (IS_VERBOSE) System.out.println(" --- RemoveProcess::checkLocal(): " + this);
			
			key = new byte[KEYSIZE];
			key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);
			hashedKey = new ByteArrayWrapper(key);
			output = ByteBuffer.allocate(2048);
			
			if (useRemote) {
				// OK, we decided that the location of key is at a remote node
				// we can transition to CONNECT_OWNER and connect to remote node
				System.out.println("--- RemoveProcess::checkLocal() Using remote");
				state = State.CONNECT_OWNER;

				try {
					// prepare the output buffer, and signal for opening a socket to remote
					generateOwnerQuery();

					socketOwner = SocketChannel.open();
					socketOwner.configureBlocking(false);
					// Send message to selector and wake up selector to process the message.
					// There is no need to set interestOps() because selector will check its queue.
					registerData(keyOwner, socketOwner, SelectionKey.OP_CONNECT);
					sel.wakeup();

				} catch (IOException e) {
					retryAtStateCheckingLocal(e);
				}


			} else {
				// OK, we decided that the location of key is at local node
				// perform appropriate action with database
				// we can transition to SEND_REQUESTER
				
				// set replyCode as appropriate and prepare output buffer
				replyValue = remove();
				if( replyValue != null ) 
					replyCode = Reply.RPY_SUCCESS.getCode(); 
				else
					replyCode = Reply.RPY_INEXISTENT.getCode();

				generateRequesterReply();

				state = State.SEND_REQUESTER;
				keyRequester.interestOps(SelectionKey.OP_WRITE);
				sel.wakeup();
			}
		}

		@Override
		public void generateRequesterReply() {
			if (output.position() != 0) {
				// This means that input received the contents of a
				// RECV_OWNER and can be directly forwarded to requester
				output.flip();
				if (IS_VERBOSE) System.out.println(" +++ RemoveProcess::generateRequesterReply() COMPLETED CASE ONE output.position()=="+output.position()+" output.limit()=="+output.limit());
				return;
			}

			// We performed a local look up.  So we fill in input with 
			// the appropriate reply to requester.
			output.put(replyCode);
			output.flip();
			if (IS_VERBOSE) System.out.println(" +++ RemoveProcess::generateRequesterReply() COMPLETED CASE TWO output.position()=="+output.position()+" output.limit()=="+output.limit());
		}

		@Override
		public void generateOwnerQuery() {
			output.position(0);
			output.put(cmd);
			output.put(key);
			output.flip();
		}
		
		@Override
		public void recvOwner() {
			// read from the socket
			try {
//				if (IS_VERBOSE) System.out.println(" +++ RemoveProcess::recvOwner() BEFORE output.position()=="+output.position()+" output.limit()=="+output.limit());
				socketOwner.read(output);
//				if (IS_VERBOSE) System.out.println(" +++ RemoveProcess::recvOwner() AFTER output.position()=="+output.position()+" output.limit()=="+output.limit());

				if (recvOwnerIsComplete()) {
					if (IS_VERBOSE) System.out.println(" +++ RemoveProcess::recvOwner() COMPLETED output.position()=="+output.position()+" output.limit()=="+output.limit());
					generateRequesterReply();
					
					state = State.SEND_REQUESTER;
					keyRequester.interestOps(SelectionKey.OP_WRITE);
					sel.wakeup();
				}
			} catch (IOException e) {
				retryAtStateCheckingLocal(e);
			}
		}

		private boolean recvOwnerIsComplete() {
			replyCode = output.get(0);
			output.position(RPYSIZE);
			output.flip();
			return true;
		}

		private byte[] remove(){
			//		System.out.println("(key.length, get key bytes): ("+key.length+
			//				", "+NodeCommands.byteArrayAsString(key) +")" );
			return map.remove(hashedKey);
		}
	}

	class UnrecogProcess implements Process {

		@Override
		public void checkLocal() {
			replyCode = NodeCommands.Reply.RPY_UNRECOGNIZED.getCode();
			output = ByteBuffer.allocate( NodeCommands.LEN_CMD_BYTES );

			generateRequesterReply();
			
			state = State.SEND_REQUESTER;
			keyRequester.interestOps(SelectionKey.OP_WRITE);
			sel.wakeup();
		}

		@Override
		public void generateRequesterReply() {
			output.position(0);
			output.put(replyCode);
			output.flip();
			if (IS_VERBOSE) System.out.println(" +++ UnrecogProcess::generateRequesterReply() COMPLETED CASE ALL output.position()=="+output.position()+" output.limit()=="+output.limit());
		}

		@Override
		public void generateOwnerQuery() {
			throw new UnsupportedOperationException("Should not call UnrecogProcess::generateOwnerQuery()");
		}

		@Override
		public void recvOwner() {
			throw new UnsupportedOperationException("Should not call UnrecogProcess::recvOwner()");
		}
	}


	public void registerData(SelectionKey keyOwner2,
			SocketChannel socketOwner2, int opConnect) {
		remote = new SocketRegisterData(keyOwner2, socketOwner2, opConnect, this);
		queue.add(remote);

	}

	@Override
	public byte[] getReply() {
		throw new UnsupportedOperationException("To be removed from Commmand interface");
	}

	@Override
	public String toString(){

		StringBuilder s = new StringBuilder();

		boolean isMatch = false;
		s.append("[command=>");
		MATCH_CMD: for (Request req: Request.values()) {
			if (req.getCode() == cmd) {
				s.append(req.toString());
				isMatch = true;
				break MATCH_CMD;
			}
		}
		
		if (!isMatch) 
			s.append(Request.CMD_UNRECOG.toString());
		
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
		if (null != input) {
			s.append("] [input.remaining()=>");
			s.append(input.remaining());
		}
		if (null != output) {
			s.append("] [output.remaining()=>");
			s.append(output.remaining());
		}
		s.append("]");

		return s.toString();
	}


}