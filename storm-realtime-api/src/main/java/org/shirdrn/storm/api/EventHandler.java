package org.shirdrn.storm.api;

import java.io.Serializable;
import java.util.Collection;

/**
 * Event handler which handles event for compute a set of
 * indicators.
 * 
 * @author Yanjun
 *
 * @param <R> Computation result
 * @param <E> Event data object
 */
public interface EventHandler<R, E> extends Serializable {

	/**
	 * Execute statistics computation, and result stat result.
	 * @param event
	 * @return
	 */
	R handle(E event) throws Exception;
	
	/**
	 * Get indicator set related to this  {@link EventHandler}.
	 * @return
	 */
	Collection<Integer> getMappedIndicators();
	
	/**
	 * Register indicators for this {@link EventHandler}.
	 * @param indicator
	 */
	void registerIndicators();
	
	/**
	 * Get event code to identify a event.
	 * @return
	 */
	String getEventCode();
}
