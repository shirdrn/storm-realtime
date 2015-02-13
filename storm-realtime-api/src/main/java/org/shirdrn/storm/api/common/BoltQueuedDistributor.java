package org.shirdrn.storm.api.common;

import java.util.concurrent.BlockingQueue;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

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
						Values values = processor.writeOut(output);
						if(values != null) {
							collector.emit(input, values);
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
