package org.shirdrn.storm.analytics.common;

import java.io.Serializable;
import java.util.Collection;

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
}
