package org.hhr.springjdbcutil.dao.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

public class NamedParamDbTemplate {
	static final Logger LOGGER = LoggerFactory.getLogger(DbTemplate.class);

	private static final String COMMA = ",";
	private static final String COLON = ":";
	private static final String EMPTY = "";
	private static final String EQUAL_COLLON = "= :";
	private static final String AND = " AND ";
	private static final String ALL = " * ";

	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	public NamedParamDbTemplate(NamedParameterJdbcTemplate namedParameterJdbcTemplate){
		this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
	}
	
	public NamedParameterJdbcTemplate getNamedTemplate() {
		return namedParameterJdbcTemplate;
	}

	public int create(String tableName, Map<String, Object> namedParameters, String sequence) {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Going to insert for table [{}] and data [{}]", tableName, namedParameters.toString());

		return execute(getCreateSql(tableName, namedParameters, sequence), namedParameters);
	}
	
	public int create(String tableName, Map<String, Object> namedParameters) {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Going to insert for table [{}] and data [{}]", tableName, namedParameters.toString());

		return execute(getCreateSql(tableName, namedParameters, null), namedParameters);
	}

	public int update(String tableName, Map<String, ?> namedParameters, Map<String, ?> whereParams) {

		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Going to update for table [{}] and data [{}]", tableName, namedParameters.toString());

		Map map = new HashMap<String, Object>(namedParameters);
		map.putAll(whereParams);

		return execute(getUpdateSql(tableName, namedParameters, whereParams), map);

	}
	public int update(String query, Map<String, Object> whereParams) {

		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Going to update for query [{}] and whereParams [{}]", query, whereParams.toString());

		return execute(query, whereParams);

	}

	public int delete(String tableName, Map<String, Object> whereParams) {

		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Deleting from table [{}] and data [{}]", tableName, whereParams.toString());

		return execute(getDeleteSql(tableName, whereParams), whereParams);

	}

	public int execute(String sql, Map<String, Object> namedParameters) {

		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Going to execute sql [{}] and data [{}]", sql, namedParameters.toString());

		int noOfUpdates = getNamedTemplate().update(sql, namedParameters);

		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Number of updates are [{}]", noOfUpdates);
		return noOfUpdates;

	}
	
	public <T> T queryForObject(String sql, Class<T> requiredType) {
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Executing sql [{}] ", sql);
		T resutls = getNamedTemplate().getJdbcOperations().queryForObject(sql, requiredType);
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Results returned [{}]", resutls);

		return resutls;
	}
	public <T> T queryForObject(String sql, Class<T> requiredType, Object ... args) {
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Executing sql [{}] ", sql);
		T resutls = null;
		try{
			resutls = getNamedTemplate().getJdbcOperations().queryForObject(sql, requiredType, args);
		}catch (EmptyResultDataAccessException e){
			LOGGER.warn("Object [{}] not found for args [{}]", requiredType, args);
		}

		if (LOGGER.isDebugEnabled()) LOGGER.debug("Results returned [{}]", resutls);

		return resutls;
	}
	
	public <T> List<T> query(String table, List<String> columnNames, Map<String, ?> whereParams, RowMapper mapper) {
		return query(getQuerySql(table, columnNames, whereParams), whereParams, mapper);
	}
	
	public <T> List<T> query(String sql, Map<String, ?> whereParams, RowMapper<T> mapper) {
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Executing sql [{}] ", sql);
		List<T> resutls = getNamedTemplate().query(sql, whereParams, mapper);
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Results returned [{}]", null != resutls ? resutls.size() : "null");

		return resutls;
	}
	
	public <T> List<T> query(String sql, Map map, Class<T> clazz) {
		SingleColumnRowMapper<T> mapper = new SingleColumnRowMapper<T>(clazz);
		return query(sql, mapper, map);
	}

	public <T> List<T> query(String sql, RowMapper<T> mapper, Map map) {
		List<T> resutls = null;
		SqlParameterSource namedParameters = new MapSqlParameterSource(map);
		
		if (LOGGER.isInfoEnabled()) LOGGER.info("Executin sql [{}] with params [{}]", sql, map.toString());
		resutls = getNamedTemplate().query(sql, namedParameters, mapper);
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Results returned [{}]", null != resutls ? resutls.size() : "null");

		return resutls;
	}

	public static String getUpdateSql(String tableName, Map<String, ?> namedParameters, Map<String, ?> whereParams) {
		// "UPDATE Employee SET age = :age WHERE empid = :empid"; //
		ArrayList<String> keys = new ArrayList<String>(namedParameters.keySet());

		String sql = "UPDATE " + tableName + " SET "+ getUpdateKeyList(keys);
		if (null != whereParams && whereParams.size() > 0) {
			sql = sql + " WHERE " +getWherePart(whereParams);
		}

		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Generated Create SQL [{}]", sql);

		return sql;
	}
	
	public static String getQuerySql(String tableName, List<String> columnNames, Map<String, ?> whereParams) {
		// SELECT * FROM Employee WHERE empid = :empid"; //

		String columns = null;
		if (null == columnNames || columnNames.size() == 0) {
			columns = ALL;
		}else{
			columns = StringUtils.join(columnNames, ",");
		}
		
		String sql = "Select " +columns + " FROM " + tableName;;
		if (null != whereParams && whereParams.size() > 0) {
			sql = sql + " WHERE " +getWherePart(whereParams);
		}
		
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Generated find SQL [{}]", sql);

		return sql;
	}

	public static String getDeleteSql(String tableName, Map<String, ?> whereParams) {
		// DELETE FROM Employee WHERE empid = :empid"; //

		String sql = "DELETE FROM " + tableName + " WHERE "+ getWherePart(whereParams);

		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Generated Create SQL [{}]", sql);

		return sql;
	}

	public static String getCreateSql(String tableName, Map<String, ?> namedParameters, String sequence) {
		// "INSERT INTO Employee (name, age, salary) VALUES (:name, :age, :salary)"//
		ArrayList<String> keys = new ArrayList<String>(namedParameters.keySet());

		String sql = "INSERT INTO " + tableName + " (" + StringUtils.join(keys, ",") + ") VALUES (" + getCreateValues(keys, COLON, sequence) + ")";
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Generated Create SQL [{}]", sql);

		return sql;
	}

	public static String getCreateValues(List<String> keys, String colon, String sequence) {
		// (name, age, salary) VALUES (:name, :age, :salary)"//
		StringBuilder sb = new StringBuilder();
		for (String key : keys) {
			if (null != sequence && key.equals("ID")) {
				sb.append(sequence);
				sequence = null;
			}else{
				sb.append(colon);
				sb.append(key);
			}
			sb.append(COMMA);
		}
		
		return StringUtils.stripEnd(sb.toString(), COMMA);
	}

	public static String getUpdateKeyList(List<String> keys) {
		// age = :age//
		StringBuilder sb = new StringBuilder();
		for (String key : keys) {
			sb.append(key);
			sb.append(EQUAL_COLLON);
			sb.append(key);
			sb.append(COMMA);
		}
		return StringUtils.stripEnd(sb.toString(), COMMA);
	}

	public static String getWherePart(Map<String, ?> whereParamsMap) {
		// age = :age//
		ArrayList<String> keys = new ArrayList<String>(whereParamsMap.keySet());
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (String key : keys) {
			if (!first) {
				sb.append(AND);
			}else{
				first = false;
			}
			sb.append(key);
			sb.append(EQUAL_COLLON);
			sb.append(key);
		}
		return sb.toString();
	}
	

}
