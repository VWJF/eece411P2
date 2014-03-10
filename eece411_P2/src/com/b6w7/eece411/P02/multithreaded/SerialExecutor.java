package com.b6w7.eece411.P02.multithreaded;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

class SerialExecutor extends Thread implements Executor {
	private final Queue<Runnable> tasks = new ArrayDeque<Runnable>();
	private Executor executor;
	private final int MAX_THREADS;
	private int numThreadSem;

	Runnable active;
	public boolean keepRunning = true;

	SerialExecutor(int numThreads) {
		this.MAX_THREADS = numThreads;
		this.numThreadSem = MAX_THREADS;
		this.executor = Executors.newFixedThreadPool(MAX_THREADS);
	}

	public void execute(final Runnable r) {
		synchronized(tasks) {
			tasks.offer(new Runnable() {
				public void run() {
					try {
						r.run();
					} finally {
						scheduleNext();
					}
				}
			});
			tasks.notifyAll();
		}
	}

	@Override
	public void run() {
		while (keepRunning ) {
			synchronized(tasks) {
				if (tasks.size() == 0) {
					try {
						tasks.wait(200);;
					} catch (InterruptedException e) {}
				}
				
				if (numThreadSem > 0) {
					numThreadSem --;
					executor.execute(active = tasks.poll());
				}
				
				if (active == null && numThreadSem > 0) {
					numThreadSem --;
					scheduleNext();
				}
			}
		}

	}

	protected void scheduleNext() {
		if ((active = tasks.poll()) != null) {
			executor.execute(active);
		}
	}
}