package org.shirdrn.storm.api;

import java.io.Serializable;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Distribute a arrived input {@link Tuple} object. Use a {@link TupleReactor}
 * you can process input {@link Tuple}s asynchronously, rather than block the
 * bolt inside spout or bolt components in the entire data pipeline.
 * 
 * @author Yanjun
 *
 * @param <IN> input tuple object, usually {@link Tuple} data
 * @param <COLLECTOR> collector object
 * @param <OUT> output data object
 */
public interface TupleReactor<IN, COLLECTOR, OUT> extends Serializable, LifecycleAware {

	/**
	 * Set the processor and parallelism of {@link Processor} instances.
	 * @param processor
	 * @param parallelism
	 */
	void setProcessorWithParallelism(Processor<IN, COLLECTOR, OUT> processor, int parallelism);
	
	/**
	 * Set the processor with default parallelism=1.
	 * @param processor
	 * @param parallelism
	 */
	void setProcessor(Processor<IN, COLLECTOR, OUT> processor);
	
	/**
	 * Distribute <code>input</code>s object to a set of worker threads to process
	 * the actually business logic in a asynchronous scenario.
	 * @param input
	 * @throws InterruptedException
	 */
	void distribute(IN input) throws InterruptedException;
	
	
	
	/**
	 * Processor is used to execute user customized business logic.
	 * A {@link TupleReactor} should distribute {@link Tuple}s to
	 * the configured {@link Processor}.</br>
	 * 
	 * Usually a {@link Processor} should be stateless.
	 * 
	 * @author Yanjun
	 *
	 * @param <IN>	IN data object
	 * @param <COLLECTOR>	collector object
	 * @param <OUT>	OUT data object
	 */
	interface Processor<IN, COLLECTOR, OUT> extends Serializable {
		
		/**
		 * Process a tuple
		 * @param input
		 * @return
		 */
		OUT process(IN input);
		
		/**
		 * Build a output data object for being emitted by this bolt
		 * @param output
		 * @return
		 */
		Values writeOut(OUT output);
	}
}
