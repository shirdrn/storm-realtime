package org.shirdrn.storm.analytics.mydis.workers;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.mydis.common.RedisSyncServer;
import org.shirdrn.storm.analytics.mydis.common.RedisSyncWorker;
import org.shirdrn.storm.analytics.mydis.constants.Constants;
import org.shirdrn.storm.analytics.mydis.constants.StatIndicators;
import org.shirdrn.storm.commons.utils.DateTimeUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;

import redis.clients.jedis.Jedis;

public class StatSyncWorker extends RedisSyncWorker {

	private static final Log LOG = LogFactory.getLog(StatSyncWorker.class);
	private final int[] indicators = new int[] {
			StatIndicators.NU, StatIndicators.AU, 
			StatIndicators.LAUNCH_TIMES,
			StatIndicators.PLAY_NU, 
			StatIndicators.PLAY_AU, 
			StatIndicators.PLAY_TIMES,
			StatIndicators.PLAY_NU_DURATION, 
			StatIndicators.PLAY_AU_DURATION
	};
	// PRIMARY KEY(indicator, hour, os_type, channel, version)
	private static final String SQL = 
			"INSERT INTO realtime_db.realtime_stat_result(indicator, hour, os_type, channel, version, count) " + 
			"VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE count=?" ;
	
	public StatSyncWorker(RedisSyncServer syncServer) {
		super(syncServer);
	}
	
	@Override
	public void process(Jedis jedis) throws Exception {
		LinkedList<String> hours = DateTimeUtils.getLatestHours(latestHours, Constants.DT_HOUR_FORMAT);
		super.addCurrentHour(hours);
		LOG.info("Sync for hours: " + hours);
		
		for(final int indicator : indicators) {
			for(final String hour : hours) {
				LOG.info("Process: indicator=" + indicator + ", hour=" + hour);
				try {
					compute(jedis, indicator, hour);
				} catch (Exception e) {
					LOG.warn("", e);
				}
			}
		}
	}

	private void compute(Jedis jedis, final int indicator, final String hour) throws Exception {
		// like: 
		// key   -> 2311010202::22::S
		// value -> 0::A-360::3.1.2
		String key = hour + Constants.REDIS_KEY_NS_SEPARATOR + 
				indicator + Constants.REDIS_KEY_NS_SEPARATOR +
				Constants.NS_STAT_HKEY; 
		Set<String> fields = jedis.hkeys(key);
		if(fields != null) {
			for(String field : fields) {
				LOG.info("key=" + key + ", field=" + field);
				String[] fieldValues = field.split(Constants.REDIS_KEY_NS_SEPARATOR);
				String strCount = jedis.hget(key, field);
				if(fieldValues.length == 3) {
					final int osType = Integer.parseInt(fieldValues[0]); 
					final String channel = fieldValues[1];
					final String version = fieldValues[2];
					final long count = Long.parseLong(strCount);
					jdbcTemplate.execute(SQL, new PreparedStatementCallback<Integer>() {
						
						@Override
						public Integer doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
							ps.setInt(1, indicator);
							ps.setString(2, DateTimeUtils.format(hour, Constants.DT_HOUR_FORMAT, Constants.DT_MINUTE_FORMAT));
							ps.setInt(3, osType);
							ps.setString(4, channel);
							ps.setString(5, version);
							ps.setLong(6, count);
							ps.setLong(7, count);
							return ps.executeUpdate();
						}
						
					});
				}
			}
		}
	}

}
