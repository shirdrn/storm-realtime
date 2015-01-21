package org.shirdrn.storm.analytics.bolts;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.constants.EventCode;
import org.shirdrn.storm.analytics.constants.EventFields;
import org.shirdrn.storm.analytics.constants.StatFields;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Distribute interested coming events.
 * 
 * @author yanjun
 */
public class EventDistributionBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(EventDistributionBolt.class);
	private OutputCollector collector;
	private Collection<String> interestedEvents;
	
	@SuppressWarnings({ "rawtypes", "serial" })
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		interestedEvents = new HashSet<String>() {{
			add(EventCode.INSTALL);
			add(EventCode.LAUNCH);
			add(EventCode.PLAY_START);
			add(EventCode.PLAY_END);
		}};
	}

	@Override
	public void execute(Tuple input) {
		String event = input.getString(0);
		LOG.debug("INPUT: event=" + event);
		JSONObject jo = null;
		try {
			jo = JSONObject.fromObject(event);
			String eventCode = jo.getString(EventFields.EVENT_CODE);
			boolean interested = isInterestedEvent(eventCode, jo);
			if(interested) {
				collector.emit(new Values(event));
				LOG.debug("Emitted: event=" + event);
			}
		} catch (Exception e) {
			LOG.warn("Illegal JSON format data: " + event);
		}
		collector.ack(input);
	}

	private boolean isInterestedEvent(String eventCode, JSONObject event) {
		return interestedEvents.contains(eventCode);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(StatFields.EVENT_DATA));
	}

}
