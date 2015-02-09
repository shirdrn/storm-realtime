package org.shirdrn.storm.api;

import java.io.Serializable;
import java.util.Collection;

/**
 * Event handler which handles event for computing a set of
 * indicators.
 * 
 * @author Yanjun
 *
 * @param <R> Computed {@link Result}
 * @param <C> Connection object
 * @param <E> Event data object
 */
public interface EventHandler<R, C, E> extends Serializable {

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

	void setConnectionManager(ConnectionManager<C> connectionManager);
	
}
