package org.shirdrn.storm.analytics.mydis.workers;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.mydis.common.RedisSyncServer;
import org.shirdrn.storm.analytics.mydis.common.RedisSyncWorker;
import org.shirdrn.storm.analytics.mydis.constants.Constants;
import org.shirdrn.storm.commons.utils.DateTimeUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;

import redis.clients.jedis.Jedis;

import com.google.common.collect.Maps;

public class UserSyncWorker extends RedisSyncWorker {

	private static final Log LOG = LogFactory.getLog(UserSyncWorker.class);
	protected int indicator;
	protected String userType;
	
	// PRIMARY KEY(indicator, hour)
	private static final String SQL = 
			"INSERT INTO realtime_db.realtime_user_result(indicator, hour, os_type, channel, version, count) " + 
			"VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE count=?" ;
	
	public UserSyncWorker(RedisSyncServer syncServer, int indicator, String userType) {
		super(syncServer);
		this.indicator = indicator;
		this.userType = userType;
	}

	@Override
	public void process(Jedis jedis) throws Exception {
		LinkedList<String> hours = DateTimeUtils.getLatestHours(latestHours, Constants.DT_HOUR_FORMAT);
		super.addCurrentHour(hours);
		LOG.info("Sync for hours: " + hours);
		
		for(final String hour : hours) {
			Map<StatObj, AtomicLong> statMap = Maps.newHashMap();
			LOG.info("Process: indicator=" + indicator + ", hour=" + hour);
			// like: 
			// key   -> 2311010202::32::AU
			// field -> 0::A-360::3.1.2::AAAAAAAAAADDDDDDDDD
			// value -> Y
			try {
				compute(jedis, statMap, hour);
			} catch (Exception e) {
				LOG.error("", e);
			}
			
			Iterator<Entry<StatObj, AtomicLong>> iter = statMap.entrySet().iterator();
			while(iter.hasNext()) {
				Entry<StatObj, AtomicLong> entry = iter.next();
				LOG.info("Processed result: [" + entry.getKey() + "] = " + entry.getValue().get());				// persist aggregated statistical result
				insertOrUpdate(hour, entry.getKey(), entry.getValue().get());
			}
		}
	}

	private void compute(Jedis jedis, Map<StatObj, AtomicLong> statMap,
			final String hour) throws Exception {
		String key = hour + Constants.REDIS_KEY_NS_SEPARATOR + 
				indicator + Constants.REDIS_KEY_NS_SEPARATOR + userType; 
		// TODO
		// performance consideration:
		// fetch results in batch, rather than fetch all once
		Set<String> fields = jedis.hkeys(key);
		if(fields != null) {
			for(String field : fields) {
				LOG.info("key=" + key + ", field=" + field);
				String[] fieldValues = field.split(Constants.REDIS_KEY_NS_SEPARATOR);
				if(fieldValues.length == 4) {
					final int osType = Integer.parseInt(fieldValues[0]); 
					final String channel = fieldValues[1];
					final String version = fieldValues[2];
					// discard udid
//					final String udid = fieldValues[3];
					StatObj so = new StatObj(osType, channel, version);
					AtomicLong c = statMap.get(so);
					if(c == null) {
						c = new AtomicLong(0);
						statMap.put(so, c);
					}
					statMap.get(so).incrementAndGet();
				}
			}
		}
	}
	
	private void insertOrUpdate(final String hour, final StatObj statObj, final long count) {
		jdbcTemplate.execute(SQL, new PreparedStatementCallback<Integer>() {
			
			@Override
			public Integer doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
				ps.setInt(1, indicator);
				ps.setString(2, DateTimeUtils.format(hour, Constants.DT_HOUR_FORMAT, Constants.DT_MINUTE_FORMAT));
				ps.setInt(3, statObj.osType);
				ps.setString(4, statObj.channel);
				ps.setString(5, statObj.version);
				ps.setLong(6, count);
				ps.setLong(7, count);
				return ps.executeUpdate();
			}
			
		});
	}
	
	class StatObj {
		final int osType;
		final String channel;
		final String version;
		
		public StatObj(int osType, String channel, String version) {
			super();
			this.osType = osType;
			this.channel = channel;
			this.version = version;
		}

		@Override
		public int hashCode() {
			return 31 * osType + 31 * channel.hashCode() + 31 * version.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			StatObj o = (StatObj) obj;
			return this.hashCode() == o.hashCode();
		}
		
		@Override
		public String toString() {
			return new StringBuffer()
				.append("osType=").append(osType).append(",")
				.append("channel=").append(channel).append(",")
				.append("version=").append(version).toString();
		}
	}

}
