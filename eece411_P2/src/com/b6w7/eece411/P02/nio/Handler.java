package com.b6w7.eece411.P02.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.b6w7.eece411.P02.multithreaded.ByteArrayWrapper;
import com.b6w7.eece411.P02.multithreaded.Command;
import com.b6w7.eece411.P02.multithreaded.JoinThread;
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
	private static final int RPYSIZE = NodeCommands.LEN_CMD_BYTES;		
	private static final int KEYSIZE = NodeCommands.LEN_KEY_BYTES;
	private static final int VALUESIZE = NodeCommands.LEN_VALUE_BYTES;
	private static final int TIMESTAMPSIZE = NodeCommands.LEN_TIMESTAMP_BYTES;
	private static final int MAX_TCP_RETRIES = 3;
	
	private static final int INTSIZE = 4;
	private static final Logger log = LoggerFactory.getLogger(ServiceReactor.class);

	byte cmd = Request.CMD_NOT_SET.getCode();
	byte[] key;
	ByteArrayWrapper hashedKey;
	byte[] value;
	byte replyCode = Reply.RPY_NOT_SET.getCode();
	byte[] replyValue;
	byte[] messageTimestamp;
	ByteBuffer byteBufferTSVector = ByteBuffer.allocate(TIMESTAMPSIZE).order(ByteOrder.BIG_ENDIAN);
	//ByteBuffer byteBufferTSVector;
	
	// when set to false, db will always reply to future requests with RPY_INTERNAL_FAILURE
	private boolean keepRunning = true;

	private int retriesLeft = MAX_TCP_RETRIES;
	public long timeStart = Long.MAX_VALUE;
	
	private final MembershipProtocol membership;
	private final PostCommand<Command> dbHandler;
	private final PostCommand<Handler> replicaHandler;

	/**
	 * The state of this {@link Command} used as a FSM
	 */
	protected State state = State.RECV_REQUESTER;

	// frequently used, so stored here for future use
	private final Handler self;
	private Selector sel;
	private Process process;
	private Queue<SocketRegisterData> queue;
	private SocketRegisterData remote;
	private InetSocketAddress owner;

	private final int serverPort;
	private final JoinThread parent;
	

	// possible states of any command
	enum State {
		RECV_REQUESTER,
		CHECKING_LOCAL,
		CONNECT_OWNER,
		SEND_OWNER,
		RECV_OWNER,
		SEND_REQUESTER, 
		DO_NOTHING, 
	}

	/**
	 * Called by TSPushProcess timer
	 * @param sel
	 * @param dbHandler
	 * @param map
	 * @param queue
	 * @param serverPort
	 * @param membership
	 * @param parent
	 * @throws IOException
	 */
	Handler(Selector sel, PostCommand<Command> dbHandler, PostCommand<Handler> replicaHandler
			, ConsistentHashing<ByteArrayWrapper, byte[]> map, Queue<SocketRegisterData> queue, int serverPort
			, MembershipProtocol membership, JoinThread parent) 
			throws IOException {

		this.parent = parent;
		this.queue = queue;
		this.sel = sel;

		if (null == map) 
			throw new IllegalArgumentException("map cannot be null");

		this.dbHandler = dbHandler;
		this.map = map;
		socketRequester = null; 
		keyRequester = null;
		
		this.membership= membership;
		this.serverPort = serverPort;
		this.process = new TSPushProcess();
		this.replicaHandler = replicaHandler;
		
		state = State.CHECKING_LOCAL;
		// keep reference to self so that nested classes can refer to the handler
		self = this;
		input.position(0);
		input.put((byte)-1);
		input.position(0);
	}

	/**
	 * Constructor
	 * @param sel
	 * @param c
	 * @param dbHandler
	 * @param map
	 * @param queue
	 * @param serverPort
	 * @param membership
	 * @param parent
	 * @throws IOException
	 */
	Handler(Selector sel, SocketChannel c, PostCommand<Command> dbHandler, PostCommand<Handler> replicaHandler
			, ConsistentHashing<ByteArrayWrapper, byte[]> map, Queue<SocketRegisterData> queue, int serverPort
			, MembershipProtocol membership, JoinThread parent) 
			throws IOException {

		this.parent = parent;
		this.queue = queue;
		this.sel = sel;

		if (null == map) 
			throw new IllegalArgumentException("map cannot be null");

		this.dbHandler = dbHandler;
		this.replicaHandler = replicaHandler;
		this.map = map;
		socketRequester = c; 
		c.configureBlocking(false);

		keyRequester = socketRequester.register(sel, 0);
		keyRequester.attach(this);
		keyRequester.interestOps(SelectionKey.OP_READ);
		sel.wakeup();

		this.membership= membership;

		this.serverPort = serverPort;
		// keep reference to self so that nested classes can refer to the handler
		self = this;
		input.position(0);
		input.put((byte)-1);
		input.position(0);
	}

	/**
	 * TSReplica Constructor
	 * @param other
	 * @param owner
	 * @param process
	 */
	public Handler(Handler other, InetSocketAddress owner, Process process) {

		this.parent = other.parent;
		this.queue = other.queue;
		this.sel = other.sel;
		this.dbHandler = other.dbHandler;
		this.replicaHandler = other.replicaHandler;
		this.map = other.map;
		socketRequester = null; 
		keyRequester = null;

		this.membership = other.membership;
		this.serverPort = other.serverPort;
		
		if(process instanceof TSReplicaPutProcess)
			this.process = new TSReplicaPutProcess();
		else if(process instanceof TSReplicaRemoveProcess)
			this.process = new TSReplicaRemoveProcess();
		
		this.owner = owner;
		this.key = other.key;
		this.value = other.value;
		this.hashedKey = other.hashedKey;
 
		state = State.CHECKING_LOCAL;
		// keep reference to self so that nested classes can refer to the handler
		self = this;
		input.position(0);
		input.put((byte)-1);
		input.position(0);
	}

	/**
	 * @return the owner
	 */
	public InetSocketAddress getOwner() {
		return owner;
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
				log.debug(" --- run(): RECV_REQUESTER {}", this);
				recvRequester();
				break;

			case CHECKING_LOCAL:
				throw new IllegalStateException(" ### CHECKING_LOCAL should not be called in run()");

			case CONNECT_OWNER:
				log.debug(" --- run(): CONNECT_OWNER {}", this);
				connectOwner();
				break;

			case SEND_OWNER:
				log.debug(" --- run(): SEND_OWNER {}", this);
				sendOwner();
				break;

			case RECV_OWNER:
				log.debug(" --- run(): RECV_OWNER {}", this);
				process.recvOwner();
				break;

			case SEND_REQUESTER:
				log.debug(" --- run(): SEND_REQUESTER {}", this);
				sendRequester();
				log.trace("MapSize: {}", map.size());
				break;
				
			case DO_NOTHING:
				log.error("     Handler::run() DO_NOTHING should not be processed");
				// do nothing
				break;
			}
		} catch (IOException ex) { /* ... */ }
	} 
	
	@Override
	public void execute() {
		switch (state) {
		case RECV_REQUESTER:
			throw new IllegalStateException(" ### RECV_REQUESTER should not be called in execute()");

		case CHECKING_LOCAL:
			log.debug(" --- execute(): CHECKING_LOCAL {}", this);
			process.checkLocal();
			break;

		case CONNECT_OWNER:
			throw new IllegalStateException(" ### CONNECT_OWNER should not be called in execute()");

		case SEND_OWNER:
			throw new IllegalStateException(" ### SEND_OWNER should not be called in execute()");

		case RECV_OWNER:
			throw new IllegalStateException(" ### RECV_OWNER should not be called in execute()");

		case SEND_REQUESTER:
			throw new IllegalStateException(" ### SEND_REQUESTER should not be called in execute()");
		default:
			break;
		}
	}


	// only call this method if we have not received enough bytes for a complete
	// operation.  This can be called multiple times until enough bytes are received.
	private void recvRequester() throws IOException {
		// read from the socket
		log.trace(" +++ Common::recvRequester() BEFORE input.position()=="+input.position()+" input.limit()=="+input.limit());
		socketRequester.read(input);
		log.trace(" +++ Common::recvRequester() AFTER input.position()=="+input.position()+" input.limit()=="+input.limit());

		if (requesterInputIsComplete()) {
			log.debug(" +++ Common::recvRequester() COMPLETE {}", this.toString());
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
			if (position >= CMDSIZE + KEYSIZE + TIMESTAMPSIZE){
				process = new TSGetProcess();
				input.position(CMDSIZE + KEYSIZE + TIMESTAMPSIZE);
				input.flip();
				return true;
			}
			return false;

		} else if (Request.CMD_TS_PUT.getCode() == cmd) {
			if (position >= CMDSIZE + KEYSIZE + VALUESIZE + TIMESTAMPSIZE) {
				process = new TSPutProcess();
				input.position(CMDSIZE + KEYSIZE + VALUESIZE + TIMESTAMPSIZE);
				input.flip();
				return true;
			}
			return false;

		} else if (Request.CMD_TS_REPLICA_PUT.getCode() == cmd) {
			if (position >= CMDSIZE + KEYSIZE + VALUESIZE + TIMESTAMPSIZE) {
				process = new TSReplicaPutProcess();
				input.position(CMDSIZE + KEYSIZE + VALUESIZE + TIMESTAMPSIZE);
				input.flip();
				return true;
			}
			return false;

		} else if (Request.CMD_TS_REMOVE.getCode() == cmd) {
			if (position >= CMDSIZE + KEYSIZE + TIMESTAMPSIZE) {
				process = new TSRemoveProcess();
				input.position(CMDSIZE + KEYSIZE + TIMESTAMPSIZE);
				input.flip();
				return true;
			}
			return false;

		} else if (Request.CMD_TS_REPLICA_REMOVE.getCode() == cmd) {
			if (position >= CMDSIZE + KEYSIZE + TIMESTAMPSIZE) {
				process = new TSReplicaRemoveProcess();
				input.position(CMDSIZE + KEYSIZE + TIMESTAMPSIZE);
				input.flip();
				return true;
			}
			return false;

		} else if (Request.CMD_TS_PUSH.getCode() == cmd) {
			process = new TSPushProcess();
			input.position(CMDSIZE + TIMESTAMPSIZE);
			input.flip();
			return true;

		} else if (Request.CMD_ANNOUNCEDEATH.getCode() == cmd) {
			process = new TSAnnounceDeathProcess();
			input.position(CMDSIZE);
			input.flip();
			return true;
			
		} else {
			process = new UnrecogProcess();
			log.warn(" ### common::requesterInputIsComplete() Unrecognized command received on wire 0x{}"
					, Integer.toString((0x100 + (int)(cmd & 0xFF)), 16).substring(1));
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
			log.debug("     Handler::connectOwner() Waiting for connectOwner to complete");
			if (socketOwner.finishConnect())  { // {System.out.print("a");}
				keyOwner = remote.key;
				if (keyOwner == null) {
					log.debug(" ### key is null");
					sel.wakeup();

				} else {
					state = State.SEND_OWNER;
					keyOwner.interestOps(SelectionKey.OP_WRITE);
					sel.wakeup();
				}
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
		log.debug(" *** *** common::sendOwner() START {}", this);

		try {
			socketOwner.write(output);
		} catch (IOException e) {
			retryAtStateCheckingLocal(e);
		}

		if (outputIsComplete()) {
			if (!socketOwner.isConnected()) {
				log.trace(" %%% sendOwner(); socketOwner.isConnected() == false!");
				retryAtStateCheckingLocal(new IllegalStateException("socket should be connected after having just written to it"));
			}
			if (!socketOwner.isOpen()) {
				log.trace(" %%% sendOwner(); socketOwner.isOpen() == false!");
				retryAtStateCheckingLocal(new IllegalStateException("socket should be open after having just written to it"));
			}
			
			log.trace("     common::sendOwner() outputIsComplete() {}", this);
			state = State.RECV_OWNER;
			keyOwner.interestOps(SelectionKey.OP_READ);
			output.position(0); // need to set to zero before entering RECV_OWNER!
			sel.wakeup();
		}
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
				log.debug(" *** sendRequest() socketRequester is still null or not open");
				return;
			}

			log.trace(" +++ Common::sendRequester() BEFORE {}", this);
			socketRequester.write(output);
			log.trace(" +++ Common::sendRequester() AFTER {}", this);

			if (outputIsComplete()) {
				log.debug(" +++ Common::sendRequester() COMPLETE {}", this);
				doNothing();
			}
			
		} catch (IOException e) {
			doNothing();
		}
	}

	private void doNothing() {
		deallocateInternalNetworkResources();
		deallocateExternalNetworkResources();
		state = State.DO_NOTHING;
		return;
	}

	private void deallocateInternalNetworkResources() {
		if (null != socketOwner) {
			try {
				socketOwner.close();
			} catch (IOException e) {}
		}
		if (null != keyOwner && keyOwner.isValid()) { keyOwner.cancel(); }
	}
	/**
	 * Unrecoverable network error occurred.  Deallocate all network resources and
	 * abort by transitioning to state ABORT
	 * @param e Exception that occurred
	 */
	private void abort(Exception e) {
		// Unknown host.  Fatal error.  Abort this command.
		retriesLeft = -1;
		log.debug(" *** Handler::abort() {}", e.getMessage());
		doNothing();
	}

	private void deallocateExternalNetworkResources() {
		if (null != socketRequester) {
			try {
				socketRequester.close();
			} catch (IOException e) {}
		}
		if (null != keyRequester && keyRequester.isValid()) { keyRequester.cancel(); }
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
	public void retryAtStateCheckingLocal(Exception e) {
		log.debug(">>>>>>>> *** *** retryAtStateCheckingLocal() START {} [retries=>{}] [process=>{}] <<<<<<<<<<", this, retriesLeft, process.getClass().toString());

		retriesLeft --;

		if (retriesLeft < 0) {
			log.debug(">>>>>>>> *** Handler::retryAtStateCheckingLocal() retriesLeft: {}.  <<<<<<<<<<", retriesLeft);
			map.shutdown(ConsistentHashing.hashKey(owner.getHostName() + ":" + owner.getPort()));

			if (process.iterativeRepeat())
				retriesLeft = 3;
		}

		log.debug("*** Handler::retryAtStateCheckingLocal() Network error in connecting to remote node. {}", e.getMessage());
		//e.printStackTrace();
		deallocateInternalNetworkResources();
		state = State.CHECKING_LOCAL;
		input.position(0);
		processRecvRequester();
	}

	private void mergeVector(int index) {
		//only perform this once
		if (null == messageTimestamp) {
			messageTimestamp = new byte[TIMESTAMPSIZE];
			messageTimestamp = Arrays.copyOfRange(
					input.array()
					, index
					, index+TIMESTAMPSIZE);

			IntBuffer intTimeStampBuffer = ByteBuffer.wrap(messageTimestamp)
											.order(ByteOrder.BIG_ENDIAN)
											.asIntBuffer();
			
			int[] backingArray = new int[TIMESTAMPSIZE/INTSIZE];
			try {
				intTimeStampBuffer.get(backingArray);
			} catch (BufferUnderflowException bue) {
				bue.printStackTrace();
				//Nothing.
			}
			membership.mergeVector(backingArray);
		}
	}
	
	private void incrLocalTime() {
		int[] updateTSVector = membership.incrementAndGetVector();

		//byteBufferTSVector = ByteBuffer.allocate(updateTSVector.length * INTSIZE).order(ByteOrder.BIG_ENDIAN);
		byteBufferTSVector.position(0);
		byteBufferTSVector.asIntBuffer().put(updateTSVector);
		byteBufferTSVector.limit(byteBufferTSVector.capacity());
	}
	
	class TSAnnounceDeathProcess implements Process {

		@Override
		public void checkLocal() {
			log.debug(" --- TSAnnounceDeathProcess::checkLocal(): {}", self);
			keepRunning = false;

			output = ByteBuffer.allocate(2048);

			incrLocalTime();

			replyCode = Reply.RPY_SUCCESS.getCode();
			
			generateRequesterReply();

			// signal to selector that we are ready to write
			state = State.SEND_REQUESTER;
			keyRequester.interestOps(SelectionKey.OP_WRITE);
			sel.wakeup();
			
			map.shutdown(null);
			parent.announceDeath();
		}

		@Override
		public void generateOwnerQuery() {
			log.debug(" +++ TSAnnounceDeathProcess::generateOwnerQuery() START {}", self);
			output.position(0);
			output.put(Request.CMD_TS_PUT_BULK.getCode());
			output.put(key);
			output.put(value);

			byteBufferTSVector.position(0);
			output.put(byteBufferTSVector);
			output.flip();
			log.debug(" +++ TSAnnounceDeathProcess::generateOwnerQuery() COMPLETE {}", self);
		}

		@Override
		public void generateRequesterReply() {
			// We performed a local look up.  So we fill in input with 
			// the appropriate reply to requester.
			output.put(replyCode);
			output.flip();
			log.debug(" +++ TSAnnounceDeathProcess::generateRequesterReply() COMPLETE LOCAL {}", self);
		}

		@Override
		public void recvOwner() {
			throw new UnsupportedOperationException(" ### should not call TSAnnounceDeathProcess::recvOwner()");
		}

		@Override
		public boolean iterativeRepeat() {
			throw new IllegalStateException(" ### should not call TSAnnounceDeathProcess::iterativeRepeat()");
		}
		
	}
	
	class TSPutProcess extends PutProcess {

		@Override
		public void checkLocal() {
			log.debug(" --- TSPutProcess::checkLocal(): {}", self);
			
			key = new byte[KEYSIZE];
			key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);
			value = new byte[VALUESIZE];
			value = Arrays.copyOfRange(input.array(), CMDSIZE+KEYSIZE, CMDSIZE+KEYSIZE+VALUESIZE);
			hashedKey = new ByteArrayWrapper(key);
			output = ByteBuffer.allocate(2048);

			if (!keepRunning) {
				doNothing();
				return;
			}

			mergeVector(CMDSIZE+KEYSIZE+VALUESIZE);
			incrLocalTime();

			// OK, we decided that the location of key is at local node
			// perform appropriate action with database
			// we can transition to SEND_REQUESTER
			
			// set replyCode as appropriate and prepare output buffer
			log.debug("--- TSPutProcess::checkLocal() ------------ Using Local --------------");
			if( put() )
				replyCode = Reply.RPY_SUCCESS.getCode(); 
			else
				replyCode = Reply.RPY_OUT_OF_SPACE.getCode();

			
			spawnReplicaPut();
			
			generateRequesterReply();

			// signal to selector that we are ready to write
			state = State.SEND_REQUESTER;
			keyRequester.interestOps(SelectionKey.OP_WRITE);
			timeStart = new Date().getTime();
			sel.wakeup();

		}
	}
	
	class PutProcess implements Process {
		
		private List<InetSocketAddress> replicaList;

		@Override
		public void checkLocal() {
			log.debug(" --- PutProcess::checkLocal(): {}", self);
			
			key = new byte[KEYSIZE];
			key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);
			value = new byte[VALUESIZE];
			value = Arrays.copyOfRange(input.array(), CMDSIZE+KEYSIZE, CMDSIZE+KEYSIZE+VALUESIZE);
			hashedKey = new ByteArrayWrapper(key);
			output = ByteBuffer.allocate(2048);

			if (!keepRunning) {
				// we reply with internal failure.  This node should no longer be servicing connections.
				replyCode = Reply.RPY_INTERNAL_FAILURE.getCode();
				generateRequesterReply();
				return;
			}
			
			incrLocalTime();
			
			// Instantiate owner list
			if (replicaList == null)
				replicaList = map.getReplicaList(hashedKey, false);

			if (retriesLeft == 3) {
				if (replicaList.size() == 0) {
					log.trace(" *** PutProcess::checkLocal() All replicas offline"); 
					// we have ran out of nodes to connect to, so internal error
					replyCode = Reply.RPY_INTERNAL_FAILURE.getCode();
					generateRequesterReply();

					// signal to selector that we are ready to write
					state = State.SEND_REQUESTER;
					keyRequester.interestOps(SelectionKey.OP_WRITE);
					sel.wakeup();
					return;
				}
				
				SEARCH_FOR_LOCAL: for (InetSocketAddress addr : replicaList) {
					log.trace("     PutProcess::checkLocal() replica considering {}", owner); 

					if (ConsistentHashing.isThisMyIpAddress(addr, serverPort)) {
						owner = addr;
						log.trace(" *** PutProcess::checkLocal() found owner who is LOCALHOST {}", owner); 

						if (!replicaList.remove(addr)) 
							log.error(" ### PutProcess::checkLocal() Corrupted database {}", addr); 
						break SEARCH_FOR_LOCAL;
					}
				}
			}
			
			if (owner == null)
				owner = replicaList.remove(0);
			
			log.trace("     PutProcess::checkLocal() replica using {}", owner); 

			if (ConsistentHashing.isThisMyIpAddress(owner, serverPort)) {
				// OK, we decided that the location of key is at local node
				// perform appropriate action with database
				// we can transition to SEND_REQUESTER
				
				// set replyCode as appropriate and prepare output buffer
				log.debug("--- PutProcess::checkLocal() ------------ Using Local --------------");
				if( put() )
					replyCode = Reply.RPY_SUCCESS.getCode(); 
				else
					replyCode = Reply.RPY_OUT_OF_SPACE.getCode();

				spawnReplicaPut();

				generateRequesterReply();

				// signal to selector that we are ready to write
				state = State.SEND_REQUESTER;
				keyRequester.interestOps(SelectionKey.OP_WRITE);
				sel.wakeup();

			} else {
				// OK, we decided that the location of key is at a remote node
				// we can transition to CONNECT_OWNER and connect to remote node
				log.debug("--- PutProcess::checkLocal() -------------- Using remote --------------");
				incrLocalTime();

				try {
					// prepare the output buffer, and signal for opening a socket to remote
					generateOwnerQuery();

					socketOwner = SocketChannel.open();
					socketOwner.configureBlocking(false);
					// Send message to selector and wake up selector to process the message.
					// There is no need to set interestOps() because selector will check its queue.
					registerData(keyOwner, socketOwner, SelectionKey.OP_CONNECT, owner);

					state = State.CONNECT_OWNER;
					timeStart = new Date().getTime();
					sel.wakeup();

				} catch (IOException e) {
					retryAtStateCheckingLocal(e);
				}
			}
		}

		@Override
		public void generateRequesterReply() {
			if (replyCode == NodeCommands.Reply.RPY_NOT_SET.getCode()) {
				// This means that we received from owner in 
				// RECV_OWNER and output can be directly forwarded to requester
				log.debug(" +++ GetProcess::generateRequesterReply() COMPLETE REMOTE {}", self);
				return;
			}
			
			// We performed a local look up.  So we fill in input with 
			// the appropriate reply to requester.
			output.put(replyCode);
			output.flip();
			log.debug(" +++ PutProcess::generateRequesterReply() COMPLETE LOCAL {}", self);
		}

		@Override
		public void generateOwnerQuery() {
			log.debug(" +++ TSPutProcess::generateOwnerQuery() START {}", self);
			output.position(0);
			output.put(Request.CMD_TS_PUT.getCode());
			output.put(key);
			output.put(value);

			byteBufferTSVector.position(0);
			output.put(byteBufferTSVector);
			output.flip();
			log.debug(" +++ TSPutProcess::generateOwnerQuery() COMPLETE {}", self);
		}

		@Override
		public void recvOwner() {
			output.limit(output.capacity());

			// read from the socket
			try {
				log.trace(" +++ PutProcess::recvOwner() BEFORE output.position()=="+output.position()+" output.limit()=="+output.limit());
				socketOwner.read(output);
				log.trace(" +++ PutProcess::recvOwner() AFTER output.position()=="+output.position()+" output.limit()=="+output.limit());

				if (recvOwnerIsComplete()) {
					log.debug(" +++ PutProcess::recvOwner() COMPLETE {}", self);
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
			output.position(RPYSIZE);
			output.flip();
			
			return true;
		}

		protected boolean put(){
			if(map.size() == MAX_MEMORY && map.containsKey(hashedKey) == false ){
				return false;

			} else {
				byte[] result = map.put(hashedKey, value);

				if(result != null) {
					// Overwriting -- we take note
					if(IS_VERBOSE) System.out.println("*** PutCommand() Replacing Key " + this.toString());
				}

				return true;
			}
		}

		@Override
		public boolean iterativeRepeat() {
			return true;
		}
	}
	
	
	class TSReplicaPutProcess extends PutProcess {

		@Override
		public void checkLocal() {
			log.debug(" --- TSReplicaPutProcess::checkLocal(): {}", self);
			
			output = ByteBuffer.allocate(2048);
			
			if (retriesLeft < 0) {
				doNothing();
				return;
			}
			
			if (!keepRunning) {
				doNothing();
				return;
			}
			
			mergeVector(CMDSIZE+KEYSIZE+VALUESIZE);
			incrLocalTime();
			
			if (cmd == Request.CMD_TS_REPLICA_PUT.getCode()) {
				// OK this is a request from another node that has arrived here
				// we need to read local timestamp and send it back
				
				key = new byte[KEYSIZE];
				key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);
				value = new byte[VALUESIZE];
				value = Arrays.copyOfRange(input.array(), CMDSIZE+KEYSIZE, CMDSIZE+KEYSIZE+VALUESIZE);
				hashedKey = new ByteArrayWrapper(key);

				// set replyCode as appropriate and prepare output buffer
				log.debug("--- TSReplicaPutProcess::checkLocal() ------------ Using Local --------------");
				if( put() )
					replyCode = Reply.RPY_SUCCESS.getCode(); 
				else
					replyCode = Reply.RPY_OUT_OF_SPACE.getCode();

				generateRequesterReply();

				// signal to selector that we are ready to write
				state = State.SEND_REQUESTER;
				keyRequester.interestOps(SelectionKey.OP_WRITE);
				timeStart = new Date().getTime();
				sel.wakeup();
				
			} else {
				// OK this is a request from this node that will be outbound
				// this was triggered by a periodic local timer
				if (owner == null){
					abort(new IllegalStateException(" ### should not have null for TSReplicaPutProcess::owner"));
					return;
				}
				
				log.debug(" --- TSReplicaPutProcess::checkLocal(): owner==[{},{}]", owner.getAddress().getHostAddress(), owner.getPort());

				incrLocalTime();
				try {
					// prepare the output buffer, and signal for opening a socket to remote
					generateOwnerQuery();

					socketOwner = SocketChannel.open();
					socketOwner.configureBlocking(false);
					// Send message to selector and wake up selector to process the message.
					// There is no need to set interestOps() because selector will check its queue.
					registerData(keyOwner, socketOwner, SelectionKey.OP_CONNECT, owner);
					
					state = State.CONNECT_OWNER;
					timeStart = new Date().getTime();
					sel.wakeup();

				} catch (IOException e) {
					retryAtStateCheckingLocal(e);
				}
			}
		}

		@Override
		public void generateOwnerQuery() {
			log.debug(" +++ TSReplicaPutProcess::generateOwnerQuery() START {}", self);
			output.position(0);
			output.put(Request.CMD_TS_REPLICA_PUT.getCode());
			output.put(key);
			output.put(value);

			byteBufferTSVector.position(0);
			output.put(byteBufferTSVector);
			output.flip();
			log.debug(" +++ TSReplicaPutProcess::generateOwnerQuery() COMPLETE {}", self);
		}

		@Override
		public void generateRequesterReply() {
			// We performed a local look up.  So we fill in input with 
			// the appropriate reply to requester.
			output.put(replyCode);
			output.flip();
			log.debug(" +++ TSReplicaPutProcess::generateRequesterReply() COMPLETE LOCAL {}", self);
		}

		@Override
		public void recvOwner() {
			output.limit(output.capacity());

			// read from the socket
			try {
				log.trace(" +++ TSReplicaPutProcess::recvOwner() BEFORE output.position()=="+output.position()+" output.limit()=="+output.limit());
				socketOwner.read(output);
				log.trace(" +++ TSReplicaPutProcess::recvOwner() AFTER output.position()=="+output.position()+" output.limit()=="+output.limit());

				if (recvOwnerIsComplete()) {
					log.debug(" +++ TSReplicaPutProcess::recvOwner() COMPLETE {}", self);
					
					deallocateInternalNetworkResources();
					doNothing();
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
			output.position(RPYSIZE);
			output.flip();
			
			return true;
		}

		@Override
		public boolean iterativeRepeat() {
			return false;
		}

		protected boolean put(){
			if(map.size() == MAX_MEMORY && map.containsKey(hashedKey) == false ){
				return false;

			} else {
				byte[] result = map.put(hashedKey, value);

				if(result != null) {
					// Overwriting -- we take note
					if(IS_VERBOSE) System.out.println("*** PutCommand() Replacing Key " + this.toString());
				}

				return true;
			}
		}
	}
	
	private void spawnReplicaPut() {
		List<InetSocketAddress> replicaList = map.getReplicaList(hashedKey, true);
		Collection<Handler> replicaSet = new HashSet<Handler>();
		
		log.debug("spawnReplicaPut() [replicaList=>{}]", replicaList);
		for (InetSocketAddress replica : replicaList) {
			assert( replica != null);
			
			log.debug("spawnReplicaPut() [replica=>{}]", replica);
			replicaSet.add(new Handler(this, replica, new TSReplicaPutProcess()));
		}
		replicaHandler.post(replicaSet);
	}
	
	private void spawnReplicaRemove() {
		List<InetSocketAddress> replicaList = map.getReplicaList(hashedKey, true);
		Collection<Handler> replicaSet = new HashSet<Handler>();

		log.debug("spawnReplicaRemove() [replicaList=>{}]", replicaList);
		for (InetSocketAddress replica : replicaList) {
			assert( replica != null);
			
			log.debug("spawnReplicaRemove() [replica=>{}]", replica);
			replicaSet.add(new Handler(this, replica, new TSReplicaRemoveProcess()));
		}
		replicaHandler.post(replicaSet);
	}
	
	class TSPushProcess implements Process {

		@Override
		public void checkLocal() {
			log.debug(" --- TSPushProcess::checkLocal(): {}", self);
			
			if (!keepRunning) {
				doNothing();
				return;
			}
			
			output = ByteBuffer.allocate(2048);

			mergeVector(CMDSIZE);
			incrLocalTime();
			
			if (cmd == Request.CMD_TS_PUSH.getCode()) {
				// OK this is a request from another node that has arrived here
				// we need to read local timestamp and send it back
				
				// set replyCode as appropriate and prepare output buffer
				log.debug("--- PutProcess::checkLocal() ------------ Using Local --------------");
				replyCode = Reply.RPY_SUCCESS.getCode(); 

				generateRequesterReply();

				// signal to selector that we are ready to write
				state = State.SEND_REQUESTER;
				keyRequester.interestOps(SelectionKey.OP_WRITE);
				sel.wakeup();
				
			} else {
				// OK this is a request from this node that will be outbound
				// this was triggered by a periodic local timer
				if (retriesLeft == 3)
					owner = map.getRandomOnlineNode();
				
				if (owner == null){
					log.debug("All nodes are offline");
					state = State.DO_NOTHING;
					return;
				}
				
				key = (owner.getAddress().getHostName() + ":" + owner.getPort()).getBytes();

				log.debug(" --- TSPushProcess::checkLocal(): map.getRandomOnlineNode()==[{},{}]", owner.getAddress().getHostAddress(), owner.getPort());

				// OK, we decided that the location of key is at a remote node
				// we can transition to CONNECT_OWNER and connect to remote node
				log.debug("--- TSPushProcess::checkLocal()");
				incrLocalTime();

				try {
					// prepare the output buffer, and signal for opening a socket to remote
					generateOwnerQuery();

					socketOwner = SocketChannel.open();
					socketOwner.configureBlocking(false);
					// Send message to selector and wake up selector to process the message.
					// There is no need to set interestOps() because selector will check its queue.
					registerData(keyOwner, socketOwner, SelectionKey.OP_CONNECT, owner);

					state = State.CONNECT_OWNER;
					timeStart = new Date().getTime();
					sel.wakeup();

				} catch (IOException e) {
					retryAtStateCheckingLocal(e);
				}
			}
		}

		@Override
		public void generateOwnerQuery() {
			log.debug(" +++ TSPushProcess::generateOwnerQuery() START    {}", self);
			output.position(0);
			output.put(Request.CMD_TS_PUSH.getCode());

			byteBufferTSVector.position(0);
			output.put(byteBufferTSVector);
			output.flip();
			log.debug(" +++ TSPushProcess::generateOwnerQuery() COMPLETE {}", self);
		}

		@Override
		public void generateRequesterReply() {
			// We performed a local look up.  So we fill in input with 
			// the appropriate reply to requester.
			output.put(replyCode);
			
			byteBufferTSVector.position(0);
			output.put(byteBufferTSVector);
			output.flip();
			log.debug(" +++ TSPushProcess::generateRequesterReply() COMPLETE LOCAL {}", self);
		}

		@Override
		public void recvOwner() {
			log.debug(" *** *** TSPushProcess::recvOwner() START {}", this);
			output.limit(output.capacity());

			// read from the socket then close the connection
			try {
				socketOwner.read(output);

				if (recvOwnerIsComplete()) {
					log.debug(" +++ TSPushProcess::recvOwner() COMPLETE {}", this);
					deallocateInternalNetworkResources();
					doNothing();
				}
			} catch (IOException e) {
				retryAtStateCheckingLocal(e);
			}
		}

		protected boolean recvOwnerIsComplete() {
			output.position(RPYSIZE);
			output.flip();
			
			return true;
		}

		@Override
		public boolean iterativeRepeat() {
			return true;
		}
	}
	
	class TSGetProcess extends GetProcess {

		@Override
		public void checkLocal() {
			log.debug(" --- TSGetProcess::checkLocal(): {}", self);

			key = new byte[KEYSIZE];
			key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);
			hashedKey = new ByteArrayWrapper(key);
			output = ByteBuffer.allocate(2048);

			if (!keepRunning) {
				// we reply with internal failure.  This node should no longer be servicing connections.
				replyCode = Reply.RPY_INTERNAL_FAILURE.getCode();
				generateRequesterReply();
				return;
			}

			mergeVector(CMDSIZE+KEYSIZE);
			incrLocalTime();
			owner = map.getSocketNodeResponsible(ConsistentHashing.hashKey(key));

			// OK, we decided that the location of key is at local node
			// perform appropriate action with database
			// we can transition to SEND_REQUESTER
			log.debug("--- GetProcess::checkLocal() ------------ Using Local --------------");

			// set replyCode as appropriate and prepare output buffer
			replyValue = get();
			if( replyValue != null )  
				replyCode = Reply.RPY_SUCCESS.getCode(); 
			else
				replyCode = Reply.RPY_INEXISTENT.getCode();

			generateRequesterReply();

			keyRequester.interestOps(SelectionKey.OP_WRITE);

			state = State.SEND_REQUESTER;
			timeStart = new Date().getTime();
			sel.wakeup();


		}
	}

	class GetProcess implements Process {

		private List<InetSocketAddress> replicaList;

		@Override
		public void checkLocal() {			
			log.debug(" --- GetProcess::checkLocal(): {}", self);

			key = new byte[KEYSIZE];
			key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);
			hashedKey = new ByteArrayWrapper(key);
			output = ByteBuffer.allocate(2048);
			
			if (!keepRunning) {
				// we reply with internal failure.  This node should no longer be servicing connections.
				replyCode = Reply.RPY_INTERNAL_FAILURE.getCode();
				generateRequesterReply();
				return;
			}

			incrLocalTime();
			
			// Instantiate owner list
			if (replicaList == null)
				replicaList = map.getReplicaList(hashedKey, false);

			if (retriesLeft == 3) {
				if (replicaList.size() == 0) {
					// we have ran out of nodes to connect to, so internal error
					replyCode = Reply.RPY_INTERNAL_FAILURE.getCode();
					generateRequesterReply();

					// signal to selector that we are ready to write
					state = State.SEND_REQUESTER;
					keyRequester.interestOps(SelectionKey.OP_WRITE);
					sel.wakeup();
					return;
				}
	
				log.trace("     GetProcess::checkLocal() replica considering {}", owner); 
				SEARCH_FOR_LOCAL: for (InetSocketAddress addr : replicaList) {
					if (ConsistentHashing.isThisMyIpAddress(addr, serverPort)) {
						owner = addr;
						log.trace(" *** GetProcess::checkLocal() found owner who is LOCALHOST {}", owner); 
						if (!replicaList.remove(addr)) 
							log.error(" ### GetProcess::checkLocal() Corrupted database {}", addr); 
						break SEARCH_FOR_LOCAL;
					}
				}
			}
			
			if (owner == null)
				owner = replicaList.remove(0);

			if (ConsistentHashing.isThisMyIpAddress(owner, serverPort) ) {
				// OK, we decided that the location of key is at local node
				// perform appropriate action with database
				// we can transition to SEND_REQUESTER
				log.debug("--- GetProcess::checkLocal() ------------ Using Local --------------");
				
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
				
			} else {
				// OK, we decided that the location of key is at a remote node
				// we can transition to CONNECT_OWNER and connect to remote node
				log.debug("--- GetProcess::checkLocal() ------------ Using Remote --------------");
				incrLocalTime();

				try {
					// prepare the output buffer, and signal for opening a socket to remote
					generateOwnerQuery();

					socketOwner = SocketChannel.open();
					socketOwner.configureBlocking(false);
					// Send message to selector and wake up selector to process the message.
					// There is no need to set interestOps() because selector will check its queue.
					registerData(keyOwner, socketOwner, SelectionKey.OP_CONNECT, owner);

					state = State.CONNECT_OWNER;
					timeStart = new Date().getTime();
					sel.wakeup();

				} catch (IOException e) {
					retryAtStateCheckingLocal(e);
				}

			}
		}

		@Override
		public void generateRequesterReply() {
			if (replyCode == NodeCommands.Reply.RPY_NOT_SET.getCode()) {
				// This means that we received from owner in 
				// RECV_OWNER and output can be directly forwarded to requester
				log.debug(" +++ GetProcess::generateRequesterReply() COMPLETE REMOTE {}", self);
				return;
			}

			// We performed a local look up.  So we fill in input with 
			// the appropriate reply to requester.
			output.put(replyCode);
			if (replyValue != null)
				output.put(replyValue);
			output.flip();
			log.debug(" +++ GetProcess::generateRequesterReply() COMPLETE LOCAL {}", self);
		}

		@Override
		public void generateOwnerQuery() {
			log.debug(" +++ TSGetProcess::generateOwnerQuery() START {}", self);
			output.position(0);
			output.put(Request.CMD_TS_GET.getCode());
			output.put(key);
			
			byteBufferTSVector.position(0);
			output.put(byteBufferTSVector);
			output.flip();
			log.debug(" +++ TSGetProcess::generateOwnerQuery() COMPLETE {}", self);
		}

		@Override
		public void recvOwner() {
			output.limit(output.capacity());

			// read from the socket
			try {
				log.trace(" +++ GetProcess::recvOwner() BEFORE output.position()=="+output.position()+" output.limit()=="+output.limit());
				socketOwner.read(output);
				log.trace(" +++ GetProcess::recvOwner() AFTER output.position()=="+output.position()+" output.limit()=="+output.limit());

				if (recvOwnerIsComplete()) {
					log.debug(" +++ GetProcess::recvOwner() COMPLETE {}", self);
					generateRequesterReply();
					
					state = State.SEND_REQUESTER;
					keyRequester.interestOps(SelectionKey.OP_WRITE);
					sel.wakeup();
				}

			} catch (IOException e) {
				retryAtStateCheckingLocal(e);
			}
		}

		protected boolean recvOwnerIsComplete() {
			if (Reply.RPY_SUCCESS.getCode() == output.get(0)) {
				if (output.position() >= RPYSIZE + VALUESIZE) {
					output.position(RPYSIZE + VALUESIZE);
					output.flip();
					return true;
				}
				return false;
			}

			output.position(RPYSIZE);
			output.flip();
			return true;
		}

		protected byte[] get(){
			byte[] val = map.get( hashedKey );
			if(val == null) {
				// NONEXISTENT -- we want to debug here
				log.debug(" *** GetCommand() Not Found {}", self);
			}
			return val;
		}

		@Override
		public boolean iterativeRepeat() {
			return true;
		}
	}

	class TSRemoveProcess extends RemoveProcess {

		@Override
		public void checkLocal() {
			log.debug(" --- TSRemoveProcess::checkLocal(): {}", self);
			
			key = new byte[KEYSIZE];
			key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);
			hashedKey = new ByteArrayWrapper(key);
			output = ByteBuffer.allocate(2048);
			
			if (!keepRunning) {
				// we reply with internal failure.  This node should no longer be servicing connections.
				replyCode = Reply.RPY_INTERNAL_FAILURE.getCode();
				generateRequesterReply();
				return;
			}

			mergeVector(CMDSIZE+KEYSIZE);
			incrLocalTime();

			// OK, we decided that the location of key is at local node
			// perform appropriate action with database
			// we can transition to SEND_REQUESTER
			log.debug("--- RemoveProcess::checkLocal() ------------ Using Local --------------");

			// set replyCode as appropriate and prepare output buffer
			replyValue = remove();
			if( replyValue != null ) 
				replyCode = Reply.RPY_SUCCESS.getCode(); 
			else
				replyCode = Reply.RPY_INEXISTENT.getCode();

			generateRequesterReply();

			keyRequester.interestOps(SelectionKey.OP_WRITE);

			state = State.SEND_REQUESTER;
			timeStart = new Date().getTime();
			sel.wakeup();
		}
	}

	class RemoveProcess implements Process {

		private List<InetSocketAddress> replicaList;

		@Override
		public void checkLocal() {
			log.debug(" --- RemoveProcess::checkLocal(): {}", self);

			key = new byte[KEYSIZE];
			key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);
			hashedKey = new ByteArrayWrapper(key);
			output = ByteBuffer.allocate(2048);
			
			if (!keepRunning) {
				// we reply with internal failure.  This node should no longer be servicing connections.
				replyCode = Reply.RPY_INTERNAL_FAILURE.getCode();
				generateRequesterReply();
				return;
			}

			incrLocalTime();

			// Instantiate owner list
			if (replicaList == null)
				replicaList = map.getReplicaList(hashedKey, false);

			if (retriesLeft == 3) {
				if (replicaList.size() == 0) {
					log.trace(" *** RemoveProcess::checkLocal() All replicas offline"); 
					// we have ran out of nodes to connect to, so internal error
					replyCode = Reply.RPY_INTERNAL_FAILURE.getCode();
					generateRequesterReply();

					// signal to selector that we are ready to write
					state = State.SEND_REQUESTER;
					keyRequester.interestOps(SelectionKey.OP_WRITE);
					sel.wakeup();
					return;
				}
				
				SEARCH_FOR_LOCAL: for (InetSocketAddress addr : replicaList) {
					log.trace("     RemoveProcess::checkLocal() replica considering {}", owner); 

					if (ConsistentHashing.isThisMyIpAddress(addr, serverPort)) {
						owner = addr;
						log.trace(" *** RemoveProcess::checkLocal() found owner who is LOCALHOST {}", owner); 

						if (!replicaList.remove(addr)) 
							log.error(" ### RemoveProcess::checkLocal() Corrupted database {}", addr); 
						break SEARCH_FOR_LOCAL;
					}
				}
			}
			
			if (owner == null)
				owner = replicaList.remove(0);
			
			if (ConsistentHashing.isThisMyIpAddress(owner, serverPort)) {
				// OK, we decided that the location of key is at local node
				// perform appropriate action with database
				// we can transition to SEND_REQUESTER
				log.debug("--- RemoveProcess::checkLocal() ------------ Using Local --------------");
				
				// set replyCode as appropriate and prepare output buffer
				replyValue = remove();
				if( replyValue != null ) 
					replyCode = Reply.RPY_SUCCESS.getCode(); 
				else
					replyCode = Reply.RPY_INEXISTENT.getCode();

				spawnReplicaRemove();
				
				generateRequesterReply();

				state = State.SEND_REQUESTER;
				keyRequester.interestOps(SelectionKey.OP_WRITE);
				sel.wakeup();

			} else {
				// OK, we decided that the location of key is at a remote node
				// we can transition to CONNECT_OWNER and connect to remote node
				log.debug("--- RemoveProcess::checkLocal() ------------ Using Remote --------------");
				incrLocalTime();

				try {
					// prepare the output buffer, and signal for opening a socket to remote
					generateOwnerQuery();

					socketOwner = SocketChannel.open();
					socketOwner.configureBlocking(false);
					// Send message to selector and wake up selector to process the message.
					// There is no need to set interestOps() because selector will check its queue.
					registerData(keyOwner, socketOwner, SelectionKey.OP_CONNECT, owner);

					state = State.CONNECT_OWNER;
					timeStart = new Date().getTime();
					sel.wakeup();

				} catch (IOException e) {
					retryAtStateCheckingLocal(e);
				}
			}
		}

		@Override
		public void generateRequesterReply() {
			if (replyCode == NodeCommands.Reply.RPY_NOT_SET.getCode()) {
				// This means that we received from owner in 
				// RECV_OWNER and output can be directly forwarded to requester
				log.debug(" +++ RemoveProcess::generateRequesterReply() COMPLETE REMOTE {}", self);
				return;
			}

			// We performed a local look up.  So we fill in input with 
			// the appropriate reply to requester.
			output.put(replyCode);
			output.flip();
			log.debug(" +++ RemoveProcess::generateRequesterReply() COMPLETE LOCAL {}", self);
		}

		@Override
		public void generateOwnerQuery() {
			output.position(0);
			output.put(Request.CMD_TS_REMOVE.getCode());
			output.put(key);
			byteBufferTSVector.position(0);
			output.put(byteBufferTSVector);
			output.flip();
		}
		
		@Override
		public void recvOwner() {
			output.limit(output.capacity());

			// read from the socket
			try {
				log.trace(" +++ RemoveProcess::recvOwner() BEFORE output.position()=="+output.position()+" output.limit()=="+output.limit());
				socketOwner.read(output);
				log.trace(" +++ RemoveProcess::recvOwner() AFTER output.position()=="+output.position()+" output.limit()=="+output.limit());

				if (recvOwnerIsComplete()) {
					log.debug(" +++ RemoveProcess::recvOwner() COMPLETE {}", self);
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
			output.position(RPYSIZE);
			output.flip();
			return true;
		}

		protected byte[] remove(){
			return map.remove(hashedKey);
		}

		@Override
		public boolean iterativeRepeat() {
			return true;
		}
	}

	class TSReplicaRemoveProcess extends RemoveProcess {

		@Override
		public void checkLocal() {
			log.debug(" --- TSReplicaRemoveProcess::checkLocal(): {}", self);
			
			output = ByteBuffer.allocate(2048);
			
			if (retriesLeft < 0) {
				doNothing();
				return;
			}
			
			if (!keepRunning) {
				doNothing();
				return;
			}
			
			mergeVector(CMDSIZE+KEYSIZE);
			incrLocalTime();
			
			if (cmd == Request.CMD_TS_REPLICA_REMOVE.getCode()) {
				// OK this is a request from another node that has arrived here
				// we need to read local timestamp and send it back
				
				key = new byte[KEYSIZE];
				key = Arrays.copyOfRange(input.array(), CMDSIZE, CMDSIZE+KEYSIZE);
				hashedKey = new ByteArrayWrapper(key);

				// set replyCode as appropriate and prepare output buffer
				// set replyCode as appropriate and prepare output buffer
				replyValue = remove();
				if( replyValue != null ) 
					replyCode = Reply.RPY_SUCCESS.getCode(); 
				else
					replyCode = Reply.RPY_INEXISTENT.getCode();

				generateRequesterReply();

				// signal to selector that we are ready to write
				state = State.SEND_REQUESTER;
				keyRequester.interestOps(SelectionKey.OP_WRITE);
				timeStart = new Date().getTime();
				sel.wakeup();
				
			} else {
				// OK this is a request from this node that will be outbound
				// this was triggered by a periodic local timer
				if (owner == null){
					abort(new IllegalStateException(" ### should not have null for TSReplicaRemoveProcess::owner"));
					return;
				}
				
				log.debug(" --- TSReplicaRemoveProcess::checkLocal(): owner==[{},{}]", owner.getAddress().getHostAddress(), owner.getPort());

				incrLocalTime();
				try {
					// prepare the output buffer, and signal for opening a socket to remote
					generateOwnerQuery();

					socketOwner = SocketChannel.open();
					socketOwner.configureBlocking(false);
					// Send message to selector and wake up selector to process the message.
					// There is no need to set interestOps() because selector will check its queue.
					registerData(keyOwner, socketOwner, SelectionKey.OP_CONNECT, owner);
					
					state = State.CONNECT_OWNER;
					timeStart = new Date().getTime();
					sel.wakeup();

				} catch (IOException e) {
					retryAtStateCheckingLocal(e);
				}
			}
		}

		@Override
		public void generateOwnerQuery() {
			log.debug(" +++ TSReplicaRemoveProcess::generateOwnerQuery() START {}", self);
			output.position(0);
			output.put(Request.CMD_TS_REPLICA_REMOVE.getCode());
			output.put(key);

			byteBufferTSVector.position(0);
			output.put(byteBufferTSVector);
			output.flip();
			log.debug(" +++ TSReplicaRemoveProcess::generateOwnerQuery() COMPLETE {}", self);
		}

		@Override
		public void generateRequesterReply() {
			// We performed a local look up.  So we fill in input with 
			// the appropriate reply to requester.
			output.put(replyCode);
			output.flip();
			log.debug(" +++ TSReplicaRemoveProcess::generateRequesterReply() COMPLETE LOCAL {}", self);
		}

		@Override
		public void recvOwner() {
			output.limit(output.capacity());

			// read from the socket
			try {
				log.trace(" +++ TSReplicaRemoveProcess::recvOwner() BEFORE output.position()=="+output.position()+" output.limit()=="+output.limit());
				socketOwner.read(output);
				log.trace(" +++ TSReplicaRemoveProcess::recvOwner() AFTER output.position()=="+output.position()+" output.limit()=="+output.limit());

				if (recvOwnerIsComplete()) {
					log.debug(" +++ TSReplicaRemoveProcess::recvOwner() COMPLETE {}", self);
					
					deallocateInternalNetworkResources();
					doNothing();
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
			output.position(RPYSIZE);
			output.flip();
			
			return true;
		}

		@Override
		public boolean iterativeRepeat() {
			return false;
		}

		protected byte[] remove(){
			return map.remove(hashedKey);
		}
	}
	
	class UnrecogProcess implements Process {

		@Override
		public void checkLocal() {
			log.debug(" --- UnrecogProcess::checkLocal(): {}", self);
			incrLocalTime();

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
			log.debug(" +++ UnrecogProcess::generateRequesterReply() COMPLETE {}", self);
		}

		@Override
		public void generateOwnerQuery() {
			throw new UnsupportedOperationException(" ### should not call UnrecogProcess::generateOwnerQuery()");
		}

		@Override
		public void recvOwner() {
			throw new UnsupportedOperationException(" ### should not call UnrecogProcess::recvOwner()");
		}

		@Override
		public boolean iterativeRepeat() {
			throw new IllegalStateException(" ### should not call UnrecogProcess::iterativeRepeat()");
		}
	}


	public void registerData(SelectionKey keyOwner2,
			SocketChannel socketOwner2, int opConnect, InetSocketAddress owner) {
		remote = new SocketRegisterData(keyOwner2, socketOwner2, opConnect, this, owner);
		queue.add(remote);

	}

	@Override
	public byte[] getReply() {
		throw new UnsupportedOperationException(" ### To be removed from Commmand interface");
	}

	@Override
	public String toString(){

		StringBuilder s = new StringBuilder();
		boolean isMatch = false;
		
		s.append("["+this.membership.current_node+":"+serverPort+"]"); 
		s.append("[command=>");
		MATCH_CMD: for (Request req: Request.values()) {
			if (req.getCode() == cmd) {
				s.append(req.toString());
				isMatch = true;
				break MATCH_CMD;
			}
		}
		if (!isMatch) { 
			s.append("###");
			s.append(Request.CMD_UNRECOG.toString());
		}
		
		s.append("] [key=>");
		if (null != key) {
			for (int i=0; i<LEN_TO_STRING_OF_KEY /*key.length*/; i++)
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
		
		isMatch = false;
		s.append("] [replyCode=>");
		MATCH_RPY: for (Reply rpy: Reply.values()) {
			if (rpy.getCode() == replyCode) {
				s.append(rpy.toString());
				isMatch = true;
				break MATCH_RPY;
			}
		}
		if (!isMatch) {
			s.append("###");
			s.append(Reply.RPY_UNRECOGNIZED.toString());
		}

		if (null != input) {
			s.append("] [input=>");
			s.append(input.position());
			s.append(",");
			s.append(input.limit());
			s.append(",");
			s.append(input.remaining());
		}

		if (null != output) {
			s.append("] [output=>");
			s.append(output.position());
			s.append(",");
			s.append(output.limit());
			s.append(",");
			s.append(output.remaining());
		}
		
		if (null != owner) {
			s.append("] [owner=>");
			s.append(owner.getHostName());
			s.append(":");
			s.append(owner.getPort());
		}
		s.append("]");
		
//		if(hashedKey != null){
//			List<InetSocketAddress> reps = map.getReplicaList(hashedKey);
//			s.append("]\n[Replicas: ");
//			s.append(reps);
//		}
//		s.append("]");
		
		return s.toString();
	}


	@Override
	public boolean equals(Object other)
	{
		if (!(other instanceof Handler))
		{
			return false;
		}
		return hashedKey.equals(((Handler)other).hashedKey) && owner.equals(((Handler) other).getOwner());
	}

	@Override
	public int hashCode()
	{
		assert(hashedKey!=null);
		
		return hashedKey.hashCode() + owner.hashCode();
	}

	public int compareTo(Handler arg0) {
		
		return hashedKey.compareTo(arg0.hashedKey) + compareSocket(owner, arg0.getOwner());
	}
	
	//Method to compare InetSocketAddress
	//Obtained from: http://stackoverflow.com/questions/6644738/java-comparator-for-inetsocketaddress
	private Integer getIp(InetSocketAddress addr) {
	    byte[] a = addr.getAddress().getAddress();
	    return ((a[0] & 0xff) << 24) | ((a[1] & 0xff) << 16) | ((a[2] & 0xff) << 8) | (a[3] & 0xff);
	}
	
	//Method to compare InetSocketAddress
	//Obtained from: http://stackoverflow.com/questions/6644738/java-comparator-for-inetsocketaddress
	private int compareSocket(InetSocketAddress o1, InetSocketAddress o2) {
	    //TODO deal with nulls
		if( o1 == null || o2 == null){
			return 0;
		}
	    if (o1 == o2) {
	        return 0;
	    } else if(o1.isUnresolved() || o2.isUnresolved()){
	        return o1.toString().compareTo(o2.toString());
	    } else {
	        int compare = getIp(o1).compareTo(getIp(o2));
	        if (compare == 0) {
	            compare = Integer.valueOf(o1.getPort()).compareTo(o2.getPort());
	        }
	        return compare;
	    }
	}
}
