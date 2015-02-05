package org.shirdrn.storm.analytics.common;


public abstract class JedisEventHandler<R, E> extends MappedEventHandler<R, E> {

	private static final long serialVersionUID = 1L;
	protected final JedisRichBolt jedisBolt;
	
	public JedisEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(eventCode);
		this.jedisBolt = jedisBolt;
	}
	
}
