package org.shirdrn.storm.api;

import java.io.Serializable;

import backtype.storm.spout.ISpout;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.task.IBolt;
import backtype.storm.task.IOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Distribute a arrived {@link Tuple} input object.
 * 
 * @author Yanjun
 *
 * @param <COLLECTOR> collector object: {@link ISpout} => {@link ISpoutOutputCollector}; 
 * 								 {@link IBolt} => {@link IOutputCollector}
 */
public interface TupleDistributor<IN, COLLECTOR, OUT> extends Serializable, LifecycleAware {

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
	
	void distribute(IN input) throws InterruptedException;
	
	
	
	/**
	 * Processor is used to execute user customized business logic.
	 * A {@link TupleDistributor} should distribute {@link Tuple}s to
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
