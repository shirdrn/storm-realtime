package org.shirdrn.storm.analytics.common;

import java.io.Serializable;
import java.util.Collection;

public interface EventHandler<R, E> extends Serializable {

	/**
	 * Execute statistics computation, and result stat result.
	 * @param event
	 * @param indicators
	 * @return
	 */
	R handle(E event, Collection<Integer> indicators) throws Exception;
	
	Collection<Integer> getMappedIndicators();
}
