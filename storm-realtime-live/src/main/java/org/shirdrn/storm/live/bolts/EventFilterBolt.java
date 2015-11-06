package org.shirdrn.storm.live.bolts;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.live.constants.Constants;
import org.shirdrn.storm.live.constants.EventCode;
import org.shirdrn.storm.live.constants.EventKeys;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EventFilterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(EventFilterBolt.class);
	private OutputCollector collector;
	private Collection<String> interestedEvents;
	
	@SuppressWarnings({ "serial", "rawtypes" })
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		interestedEvents = new HashSet<String>() {{
			add(EventCode.ENTER);
			add(EventCode.HEART_BEAT);
		}};
	}

	@Override
	public void execute(Tuple input) {
		try {
			String event = input.getString(0);
			JSONObject data = JSONObject.fromObject(event);
			if(data.containsKey(EventKeys.EVENT_CODE) && data.containsKey(EventKeys.ROOM_ID)) {
				String eventCode = data.getString(EventKeys.EVENT_CODE);
				if(interestedEvents.contains(eventCode)) {
					String liveRoomId = data.getString(EventKeys.ROOM_ID);
					collector.emit(input, new Values(liveRoomId, data));
				}
			}
		} catch (Exception e) {
			LOG.warn("Abnormal JSON format event data: ", e);
		} finally {
			collector.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Constants.FIELD_LIVE_ROOM_ID, Constants.FIELD_EVENT_PKT));
	}

}
