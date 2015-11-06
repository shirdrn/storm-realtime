package org.shirdrn.storm.live.handler;

import java.util.TreeSet;

import org.shirdrn.storm.api.ConnectionManager;
import org.shirdrn.storm.live.common.HeartbeatEvent;
import org.shirdrn.storm.live.common.JedisRichBolt;

import redis.clients.jedis.Jedis;

public abstract class AbstractHeartbeatEventHandler extends JedisEventHandler<TreeSet<Void>, Jedis, HeartbeatEvent> {

	private static final long serialVersionUID = 1L;
	protected final JedisRichBolt jedisBolt;
	protected transient ConnectionManager<Jedis> connectionManager;
	
	public AbstractHeartbeatEventHandler(JedisRichBolt jedisBolt, String eventCode) {
		super(eventCode);
		this.jedisBolt = jedisBolt;
	}

}
