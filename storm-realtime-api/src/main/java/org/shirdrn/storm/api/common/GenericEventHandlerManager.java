package org.shirdrn.storm.api.common;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.api.EventHandler;
import org.shirdrn.storm.api.EventHandlerManager;
import org.shirdrn.storm.api.Result;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Generic event handler manager.
 * 
 * @author Yanjun
 *
 * @param <RESULT> Computed {@link Result}
 * @param <CONNECTION> Connection object
 * @param <EVENTE> Event data object
 */
public class GenericEventHandlerManager<RESULT, CONNECTION, EVENTE> implements EventHandlerManager<RESULT, CONNECTION, EVENTE> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(GenericEventHandlerManager.class);
	private final Collection<String> interestedEvents = Sets.newHashSet();
	protected final Map<String, EventHandler<RESULT, CONNECTION, EVENTE>> eventHandlers = Maps.newHashMap();
	
	@Override
	public void interestEvent(String eventCode) {
		interestedEvents.add(eventCode);		
	}

	@Override
	public boolean isInterestedEvent(String eventCode) {
		return interestedEvents.contains(eventCode);
	}

	@Override
	public void mapping(String eventCode, EventHandler<RESULT, CONNECTION, EVENTE> eventHandler) {
		// register mappings: event-->EventHandler
		eventHandlers.put(eventCode, eventHandler);
		LOG.info("Mapped event handler: " + eventCode + " -> " + eventHandler);
	}

	@Override
	public EventHandler<RESULT, CONNECTION, EVENTE> getEventHandler(String eventCode) {
		return eventHandlers.get(eventCode);
	}

	@Override
	public void initialize() {
		// register indicators for each EventHandler
		Preconditions.checkArgument(!eventHandlers.isEmpty(), "Never set event handlers!!!");
		for(EventHandler<RESULT, CONNECTION, EVENTE> handler : eventHandlers.values()) {
			handler.registerIndicators();
			LOG.info("Indicator registered for: " + handler);
		}
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

}
