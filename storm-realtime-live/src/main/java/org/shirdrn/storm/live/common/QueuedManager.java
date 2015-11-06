package org.shirdrn.storm.live.common;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public abstract class QueuedManager<E> extends Thread {
	
	private final BlockingDeque<E> q;
	
	public QueuedManager() {
		q = new LinkedBlockingDeque<E>();
	}
	
	public void addLast(E input) {
		try {
			q.putLast(input);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public BlockingDeque<E> getQueue() {
		return q;
	}
	
}
