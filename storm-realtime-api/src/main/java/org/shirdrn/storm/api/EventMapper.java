package org.shirdrn.storm.api;

import java.io.Serializable;

/**
 * Map event code to a specified {@link EventHandler} instance.
 * 
 * @author Yanjun
 *
 * @param <RESULT> Computed {@link Result}
 * @param <CONNECTION> Connection object
 * @param <EVENT> Event data object
 */
public interface EventMapper<RESULT, CONNECTION, EVENT> extends Serializable {

	void mapping(String eventCode, EventHandler<RESULT, CONNECTION, EVENT> eventHandler);
	EventHandler<RESULT, CONNECTION, EVENT> getEventHandler(String eventCode);
}
