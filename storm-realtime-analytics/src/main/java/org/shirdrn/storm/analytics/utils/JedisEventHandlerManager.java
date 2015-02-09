package org.shirdrn.storm.analytics.utils;

import java.util.TreeSet;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.api.ConnectionManager;
import org.shirdrn.storm.api.EventHandler;
import org.shirdrn.storm.api.Result;
import org.shirdrn.storm.api.common.GenericEventHandlerManager;

import redis.clients.jedis.Jedis;

public class JedisEventHandlerManager extends GenericEventHandlerManager<TreeSet<Result>, Jedis, JSONObject> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(JedisEventHandlerManager.class);
	
	@Override
	public void initialize() {
		super.initialize();
		ConnectionManager<Jedis> connectionManager = new JedisConnectionManager(RealtimeUtils.getConfiguration());
		for(EventHandler<TreeSet<Result>, Jedis, JSONObject> handler : eventHandlers.values()) {
			handler.setConnectionManager(connectionManager);
			LOG.info("Initialize event handler: " + handler);
		}
	}

}
