package org.shirdrn.storm.api.common;

import java.util.concurrent.BlockingQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SpoutQueuedDistributor<OUT> extends QueuedDistributor<Tuple, SpoutOutputCollector, OUT> {

	private static final long serialVersionUID = 1L;

	public SpoutQueuedDistributor(SpoutOutputCollector collector) {
		super(collector);
	}
	
	public SpoutQueuedDistributor(SpoutOutputCollector collector,  BlockingQueue<Tuple> queue) {
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
