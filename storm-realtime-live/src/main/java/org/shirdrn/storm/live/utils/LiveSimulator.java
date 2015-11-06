package org.shirdrn.storm.live.utils;

import java.util.Map;

import net.sf.json.JSONObject;

import org.shirdrn.storm.live.constants.Constants;
import org.shirdrn.storm.live.constants.EventKeys;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class LiveSimulator {

	public static BaseRichSpout getSimulatedSpout() {
		return new TestSpout();
	}
	
	public static class TestSpout extends BaseRichSpout {

		private static final long serialVersionUID = 1L;
		private String[] events;
		int pointer = 0;
		SpoutOutputCollector collector;
		
		@SuppressWarnings("rawtypes")
		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.collector = collector;
			events = new String[] {
					
					// heart beat
					"{\"event_code\":\"100002\",\"room_id\":\"1000000000\",\"fragment_id\":\"2000000000\",\"udid\":\"AAA0\",\"event_time\":\"2015-09-11 16:01:17\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000001\",\"fragment_id\":\"2000000001\",\"udid\":\"BBB0\",\"event_time\":\"2015-09-11 16:01:47\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000002\",\"fragment_id\":\"2000000002\",\"udid\":\"CCC0\",\"event_time\":\"2015-09-11 16:01:22\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000003\",\"fragment_id\":\"2000000003\",\"udid\":\"DDD\",\"event_time\":\"2015-09-11 16:01:52\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000000\",\"fragment_id\":\"2000000000\",\"udid\":\"AAA1\",\"event_time\":\"2015-09-11 16:01:17\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000001\",\"fragment_id\":\"2000000001\",\"udid\":\"BBB1\",\"event_time\":\"2015-09-11 16:01:47\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000002\",\"fragment_id\":\"2000000002\",\"udid\":\"CCC1\",\"event_time\":\"2015-09-11 16:01:22\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000000\",\"fragment_id\":\"2000000000\",\"udid\":\"AAA2\",\"event_time\":\"2015-09-11 16:01:17\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000000\",\"fragment_id\":\"2000000000\",\"udid\":\"AAA3\",\"event_time\":\"2015-09-11 16:01:17\"}",
					
					"{\"event_code\":\"100002\",\"room_id\":\"1000000001\",\"fragment_id\":\"2000000001\",\"udid\":\"BBB2\",\"event_time\":\"2015-09-11 16:01:47\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000001\",\"fragment_id\":\"2000000001\",\"udid\":\"BBB3\",\"event_time\":\"2015-09-11 16:01:47\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000001\",\"fragment_id\":\"2000000001\",\"udid\":\"BBB4\",\"event_time\":\"2015-09-11 16:01:47\"}",
					
					"{\"event_code\":\"100002\",\"room_id\":\"1000000002\",\"fragment_id\":\"2000000002\",\"udid\":\"CCC2\",\"event_time\":\"2015-09-11 16:01:22\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000002\",\"fragment_id\":\"2000000002\",\"udid\":\"CCC3\",\"event_time\":\"2015-09-11 16:01:22\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000002\",\"fragment_id\":\"2000000002\",\"udid\":\"CCC4\",\"event_time\":\"2015-09-11 16:01:22\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000002\",\"fragment_id\":\"2000000002\",\"udid\":\"CCC5\",\"event_time\":\"2015-09-11 16:01:22\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000002\",\"fragment_id\":\"2000000002\",\"udid\":\"CCC6\",\"event_time\":\"2015-09-11 16:01:22\"}",
					
					"{\"event_code\":\"100002\",\"room_id\":\"1000000004\",\"fragment_id\":\"2000000004\",\"udid\":\"EEE\",\"event_time\":\"2015-09-11 16:01:32\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000005\",\"fragment_id\":\"2000000005\",\"udid\":\"FFF\",\"event_time\":\"2015-09-11 16:01:37\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000006\",\"fragment_id\":\"2000000006\",\"udid\":\"GGG\",\"event_time\":\"2015-09-11 16:01:27\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000007\",\"fragment_id\":\"2000000007\",\"udid\":\"HHH\",\"event_time\":\"2015-09-11 16:01:42\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000008\",\"fragment_id\":\"2000000008\",\"udid\":\"III\",\"event_time\":\"2015-09-11 16:01:32\"}",
					"{\"event_code\":\"100002\",\"room_id\":\"1000000009\",\"fragment_id\":\"2000000009\",\"udid\":\"JJJ\",\"event_time\":\"2015-09-11 16:01:57\"}",
					
			};
			
		}

		private String preEventCode = "";
		
		@Override
		public void nextTuple() {
			if(pointer < events.length) {
				String data = events[pointer];
				collector.emit(new Values(data));
				++pointer;
				Utils.sleep(5000);
				
				JSONObject json = JSONObject.fromObject(data);
				String eventCode = json.getString(EventKeys.EVENT_CODE);
				if(!eventCode.equals(preEventCode)) {
					preEventCode = eventCode;
					Utils.sleep(5000);
				}
			} else {
				Utils.sleep(100000);
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields(Constants.FIELD_EVENT_PKT));			
		}
		
	}
}
