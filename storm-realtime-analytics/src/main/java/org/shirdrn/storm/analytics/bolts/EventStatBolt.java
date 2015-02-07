package org.shirdrn.storm.analytics.bolts;

import java.util.Map;
import java.util.TreeSet;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.JedisEventHandler;
import org.shirdrn.storm.analytics.common.JedisRichBolt;
import org.shirdrn.storm.analytics.constants.EventCode;
import org.shirdrn.storm.analytics.constants.EventFields;
import org.shirdrn.storm.analytics.constants.StatFields;
import org.shirdrn.storm.analytics.handlers.InstallEventHandler;
import org.shirdrn.storm.analytics.handlers.OpenEventHandler;
import org.shirdrn.storm.analytics.handlers.PlayEndEventHandler;
import org.shirdrn.storm.analytics.handlers.PlayStartEventHandler;
import org.shirdrn.storm.api.Result;
import org.shirdrn.storm.api.EventHandler;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Maps;

/**
 * Real-time statistics. Some data may obtain from other source base,
 * such as querying user device information, after doing that compute
 * statistics record and emit it to next bolt to persist statistics result.
 * 
 * @author Yanjun
 */
public class EventStatBolt extends JedisRichBolt {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(EventStatBolt.class);
	private final Map<String, EventHandler<?, ?>> eventHandlers = Maps.newHashMap();
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		// register mappings: event-->EventHandler
		eventHandlers.put(EventCode.PLAY_START, new PlayStartEventHandler(this, EventCode.PLAY_START));
		eventHandlers.put(EventCode.PLAY_END, new PlayEndEventHandler(this, EventCode.PLAY_END));
		eventHandlers.put(EventCode.OPEN, new OpenEventHandler(this, EventCode.OPEN));
		eventHandlers.put(EventCode.INSTALL, new InstallEventHandler(this, EventCode.INSTALL));
		
		// register indicators for each EventHandler
		for(EventHandler<?, ?> handler : eventHandlers.values()) {
			handler.registerIndicators();
			LOG.info("Indicator registered for: " + handler);
		}
	}
	
	@Override
	public void execute(Tuple input) {
		String event = input.getString(0);
		LOG.debug("INPUT: event=" + event);
		JSONObject eventData = JSONObject.fromObject(event);
		String eventCode = eventData.getString(EventFields.EVENT_CODE);
		EventHandler<?, ?> handler = eventHandlers.get(eventCode);
		LOG.debug("Get handler: handler=" + handler);
		
		if(handler != null) {
			JedisEventHandler h = (JedisEventHandler) handler;
			try {
				TreeSet<Result> results = h.handle(eventData);
				for(Result result : results) {
					collector.emit(input, new Values(result.getIndicator(), result));
					LOG.debug("Emitted: results=" + results);
				}
			} catch (Exception e) {
				LOG.warn("Fail to handle: handler=" + h + ", indicators=" + h.getMappedIndicators() + ", event=" + eventData, e);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(StatFields.STAT_INDICATOR, StatFields.STAT_RESULT));
	}

}
