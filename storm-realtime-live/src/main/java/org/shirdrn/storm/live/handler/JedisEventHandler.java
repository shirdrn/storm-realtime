package org.shirdrn.storm.live.handler;

import org.shirdrn.storm.api.SimpleEventHandler;

public abstract class JedisEventHandler<RESULT, CONNECTION, EVENT> implements SimpleEventHandler<RESULT, EVENT> {

	private static final long serialVersionUID = 1L;
	protected final String eventCode;
	
	public JedisEventHandler(String eventCode) {
		this.eventCode = eventCode;
	}
	
	@Override
	public String getEventCode() {
		return eventCode;
	}

}
