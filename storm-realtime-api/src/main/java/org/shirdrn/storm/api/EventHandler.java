package org.shirdrn.storm.api;

import java.io.Serializable;
import java.util.Collection;

/**
 * Event handler which handles event for computing a set of
 * indicators.
 * 
 * @author Yanjun
 *
 * @param <RESULT> Computed {@link Result}
 * @param <CONNECTION> Connection object
 * @param <EVENT> Event data object
 */
public interface EventHandler<RESULT, CONNECTION, EVENT> extends SimpleEventHandler<RESULT, EVENT>, Serializable {
	
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
	 * Set connection manager.
	 */
	void setConnectionManager(ConnectionManager<CONNECTION> connectionManager);
	
}
