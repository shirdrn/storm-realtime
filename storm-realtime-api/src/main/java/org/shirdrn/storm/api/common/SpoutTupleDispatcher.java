package org.shirdrn.storm.api.common;

import java.util.concurrent.BlockingQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Tuple dispatcher for spout component, which is asynchronous tuple distributor.
 * 
 * @author Yanjun
 *
 * @param <OUT> output data object
 */
public class SpoutTupleDispatcher<OUT> extends QueuedTupleDispatcher<Tuple, SpoutOutputCollector, OUT> {

	private static final long serialVersionUID = 1L;

	public SpoutTupleDispatcher(SpoutOutputCollector collector) {
		super(collector);
	}
	
	public SpoutTupleDispatcher(SpoutOutputCollector collector,  BlockingQueue<Tuple> queue) {
		super(collector);
	}

	@Override
	protected Thread newProcessorRunner() {
		return new ProcessorRunner();
	}

	private final class ProcessorRunner extends Thread {
		
		@Override
		public void run() {
			while(true) {
				try {
					Tuple input = queue.take();
					OUT output = processor.process(input);
					Values values = processor.writeOut(output);
					if(values != null) {
						collector.emit(values);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
	}
}
