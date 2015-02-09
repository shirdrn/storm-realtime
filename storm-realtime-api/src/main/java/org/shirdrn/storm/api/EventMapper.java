package org.shirdrn.storm.api;

import java.io.Serializable;

/**
 * Map event code to a specified {@link EventHandler} instance.
 * 
 * @author Yanjun
 *
 * @param <R> Computed {@link Result}
 * @param <C> Connection object
 * @param <E> Event data object
 */
public interface EventMapper<R, C, E> extends Serializable {

	void mapping(String eventCode, EventHandler<R, C, E> eventHandler);
	EventHandler<R, C, E> getEventHandler(String eventCode);
}
