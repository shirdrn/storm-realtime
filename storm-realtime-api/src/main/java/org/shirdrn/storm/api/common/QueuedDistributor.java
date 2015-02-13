package org.shirdrn.storm.api.common;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.commons.utils.ThreadPoolUtils;

import com.google.common.base.Preconditions;

public abstract class QueuedDistributor<IN, COLLECTOR, OUT> extends GenericDistributor<IN, COLLECTOR, OUT> {
	
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(QueuedDistributor.class);
	private ExecutorService executorService;
	protected final BlockingQueue<IN> queue;

	public QueuedDistributor(COLLECTOR collector) {
		super(collector);
		this.queue = new LinkedBlockingQueue<IN>();
	}
	
	public QueuedDistributor(COLLECTOR collector, BlockingQueue<IN> queue) {
		super(collector);
		this.queue = queue;
	}
	
	@Override
	public void distribute(IN input) throws InterruptedException {
		queue.put(input);
	}
	
	@Override
	public void start() {
		Preconditions.checkArgument(processor != null, "Never set a processor for the distributor!");
		executorService = ThreadPoolUtils.newCachedThreadPool("DISTRIBUTOR");
		for (int i = 0; i < parallelism; i++) {
			Thread runner = newProcessorRunner();
			executorService.execute(runner);
			LOG.info("Processor runner started: " + runner);
		}
	}
	
	@Override
	public void stop() {
		executorService.shutdown();
	}
	
	protected abstract Thread newProcessorRunner();
	
}
