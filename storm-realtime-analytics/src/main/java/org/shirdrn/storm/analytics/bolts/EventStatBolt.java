package org.shirdrn.storm.analytics.bolts;

import java.util.Map;
import java.util.TreeSet;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.constants.EventFields;
import org.shirdrn.storm.analytics.constants.StatFields;
import org.shirdrn.storm.analytics.utils.RealtimeUtils;
import org.shirdrn.storm.api.EventHandler;
import org.shirdrn.storm.api.EventHandlerManager;
import org.shirdrn.storm.api.Result;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Real-time statistics. Some data may obtain from other source base,
 * such as querying user device information, after doing that compute
 * statistics record and emit it to next bolt to persist statistics result.
 * 
 * @author Yanjun
 */
public class EventStatBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(EventStatBolt.class);
	private EventHandlerManager<TreeSet<Result>, Jedis, JSONObject> eventHandlerManager;
	private OutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		eventHandlerManager = RealtimeUtils.getEventHandlerManager();
	}
	
	@Override
	public void execute(Tuple input) {
		String event = input.getString(0);
		LOG.debug("INPUT: event=" + event);
		JSONObject eventData = JSONObject.fromObject(event);
		String eventCode = eventData.getString(EventFields.EVENT_CODE);
		EventHandler<TreeSet<Result>, Jedis, JSONObject> handler = eventHandlerManager.getEventHandler(eventCode);
		LOG.debug("Get handler: handler=" + handler);
		
		if(handler != null) {
			try {
				TreeSet<Result> results = handler.handle(eventData);
				for(Result result : results) {
					collector.emit(input, new Values(result.getIndicator(), result));
					LOG.debug("Emitted: results=" + results);
					collector.ack(input);
				}
			} catch (Exception e) {
				LOG.warn("Fail to handle: handler=" + handler + ", indicators=" + handler.getMappedIndicators() + ", event=" + eventData, e);
			}
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(StatFields.STAT_INDICATOR, StatFields.STAT_RESULT));
	}

}
