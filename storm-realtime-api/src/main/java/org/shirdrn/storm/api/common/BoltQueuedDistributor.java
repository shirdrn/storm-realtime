package org.shirdrn.storm.api.common;

import java.util.concurrent.BlockingQueue;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class BoltQueuedDistributor<OUT> extends QueuedDistributor<Tuple, OutputCollector, OUT> {

	private static final long serialVersionUID = 1L;

	public BoltQueuedDistributor(OutputCollector collector) {
		super(collector);
	}
	
	public BoltQueuedDistributor(OutputCollector collector,  BlockingQueue<Tuple> queue) {
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
				Tuple input = null;
				try {
					input = queue.take();
					if(input != null) {
						OUT output = processor.process(input);
						if(output != null) {
							collector.emit(input, processor.writeOut(output));
						}
						collector.ack(input);
					}
				} catch (Exception e) {
					collector.fail(input);
				}
			}
		}
		
	}
}
