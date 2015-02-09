package org.shirdrn.storm.api;


/**
 * Manage events related meta data, such as event codes, the mapping from event code
 * to event handler, etc.
 * 
 * @author Yanjun
 * 
 * @param <R> Computed {@link Result}
 * @param <C> Connection object
 * @param <E> Event data object
 */
public interface EventHandlerManager<R, C, E> extends EventInteresteable, EventMapper<R, C, E> {

	/**
	 * Initialize a {@link EventHandlerManager} object.
	 */
	void initialize();
}
