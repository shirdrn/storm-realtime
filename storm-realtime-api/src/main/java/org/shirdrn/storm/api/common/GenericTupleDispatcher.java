package org.shirdrn.storm.api.common;

import org.shirdrn.storm.api.TupleDispatcher;

import backtype.storm.tuple.Tuple;

/**
 * Generic tuple dispatcher abstraction.
 * 
 * @author Yanjun
 *
 * @param <IN> input tuple object, usually {@link Tuple} data
 * @param <COLLECTOR> collector object
 * @param <OUT> output data object
 */
public abstract class GenericTupleDispatcher<IN, COLLECTOR, OUT> implements TupleDispatcher<IN, COLLECTOR, OUT> {

	private static final long serialVersionUID = 1L;
	protected final COLLECTOR collector;
	protected Processor<IN, COLLECTOR, OUT> processor;
	protected int parallelism = 1;

	public GenericTupleDispatcher(COLLECTOR collector) {
		super();
		this.collector = collector;
	}
	
	@Override
	public void setProcessorWithParallelism(Processor<IN, COLLECTOR, OUT> processor, int parallelism) {
		this.processor = processor;
		this.parallelism = parallelism;		
	}
	
	@Override
	public void setProcessor(Processor<IN, COLLECTOR, OUT> processor) {
		setProcessorWithParallelism(processor, 1);	
	}
	
}
