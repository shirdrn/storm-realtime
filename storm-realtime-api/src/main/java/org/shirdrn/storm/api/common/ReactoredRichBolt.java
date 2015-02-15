package org.shirdrn.storm.api.common;

import java.util.Map;

import org.shirdrn.storm.api.TupleReactor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;

public abstract class ReactoredRichBolt<IN, OUT> extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	protected OutputCollector collector;
	protected TupleReactor<IN, OutputCollector, OUT> tupleReactor;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

}
