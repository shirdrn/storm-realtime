package org.shirdrn.storm.api.common;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.api.TupleReactor;
import org.shirdrn.storm.commons.utils.ThreadPoolUtils;

import backtype.storm.tuple.Tuple;

import com.google.common.base.Preconditions;

/**
 * Asynchronous {@link TupleReactor} based on a {@link BlockingQueue} caching
 * mechanism.
 * 
 * @author Yanjun
 *
 * @param <IN> input tuple object, usually {@link Tuple} data
 * @param <COLLECTOR> collector object
 * @param <OUT> output data object
 */
public abstract class QueuedTupleReactor<IN, COLLECTOR, OUT> extends GenericTupleReactor<IN, COLLECTOR, OUT> {
	
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(QueuedTupleReactor.class);
	private ExecutorService executorService;
	protected final BlockingQueue<IN> queue;

	public QueuedTupleReactor(COLLECTOR collector) {
		super(collector);
		this.queue = new LinkedBlockingQueue<IN>();
	}
	
	public QueuedTupleReactor(COLLECTOR collector, BlockingQueue<IN> queue) {
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
	
	/**
	 * Implements and creates a thread to process tuples.
	 * @return
	 */
	protected abstract Thread newProcessorRunner();
	
}
