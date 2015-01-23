package org.shirdrn.storm.analytics.utils;

import java.util.Arrays;

import joptsimple.internal.Strings;

import org.apache.commons.configuration.Configuration;
import org.shirdrn.storm.commons.constants.Keys;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.base.BaseRichSpout;

import com.google.common.base.Preconditions;

public class StormComponentFactory {

	public static BaseRichSpout newKafkaSpout(String topic, Configuration config) {
		String[] zks = config.getStringArray(Keys.KAFKA_ZK_SERVERS);
		String zkServers = Strings.join(zks, ",");
		String zkRoot = config.getString(Keys.STORM_ZK_ROOT); 
		boolean forceFromStart = config.getBoolean(Keys.STORM_KAFKA_FORCE_FROM_START, false);
		String clientId = config.getString(Keys.KAFKA_CLIENT_ID);
		Preconditions.checkArgument(clientId != null, "Kafka client ID MUST NOT be null!");
		
		// Configure Kafka
		BrokerHosts brokerHosts = new ZkHosts(zkServers);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, clientId);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.forceFromStart = forceFromStart;
		
		// Configure Storm
		String[] zkHosts = config.getStringArray(Keys.STORM_ZK_HOSTS);
		int zkPort = config.getInt(Keys.STORM_ZK_PORT, 2181);
		spoutConf.zkServers = Arrays.asList(zkHosts);
		spoutConf.zkPort = zkPort;
		return new KafkaSpout(spoutConf);
	}
	
}
