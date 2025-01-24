package main.com.liferay.portal.db.partition; /**
 * Copyright (c) 2000-present Liferay, Inc. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 */

import main.com.liferay.portal.db.partition.configuration.ConfigurationProperties;
import main.com.liferay.portal.db.partition.configuration.ConfigurationHandler;
import main.com.liferay.portal.db.partition.util.GetterUtil;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

/**
 * @author Alberto Chaparro
 */
public class MigrateToDBPartition {

	public static void main(String[] args) throws Exception {
		System.out.println("*** Start migrating companies to DB Partition ***");

		if (args.length != 4) {
			System.out.println("Database name, webId from default company, user and password are required");

			return;
		}

		_defaultSchemaName = args[0];
		_defaultWebId = args[1];

		Class.forName(JDBC_DRIVER).newInstance();

		try {
			_connection = DriverManager.getConnection(
				JDBC_URL1 + _defaultSchemaName + JDBC_URL2, args[2], args[3]);

			List<Long> companyIds = _getNonDefaultCompanyIds();

			_defaultCompanyId = _getDefaultCompanyId();

			for (Long companyId : companyIds) {
				System.out.println("** Migrating company with id " + companyId);

				_createSchema(companyId);
			}

			System.out.println("** Migrating configurations");
			_moveConfigurationData(companyIds);
		}
		finally {
			if (_connection != null) {
				_connection.close();
			}
		}

		System.out.println("*** End migrating companies to DB Partition ***");
	}

	private static void _createSchema(long companyId)
		throws Exception {

		try (PreparedStatement preparedStatement = _connection.prepareStatement(
			"create schema " + _getSchemaName(companyId) +
				" character set utf8")) {

			preparedStatement.executeUpdate();

			System.out.println(
				"Schema " + _getSchemaName(companyId) + " created");

			DatabaseMetaData databaseMetaData = _connection.getMetaData();

			List<Long> companyIds = _getCompanyIds();

			try (ResultSet resultSet = databaseMetaData.getTables(
				_connection.getCatalog(), _connection.getSchema(), null,
				new String[]{"TABLE"});
				 Statement statement = _connection.createStatement()) {

				while (resultSet.next()) {
					String tableName = resultSet.getString("TABLE_NAME");

					if (_isControlTable(tableName)) {
						statement.executeUpdate(
							_getCreateView(companyId, tableName));

						continue;
					}

					if (_isObjectTable(companyIds, tableName)) {
						if (tableName.contains(String.valueOf(companyId))) {
							statement.executeUpdate(
								_getCreateTable(companyId, tableName));

							_moveData(companyId, true, tableName, statement, "");
						}

						continue;
					}

					statement.executeUpdate(
						_getCreateTable(companyId, tableName));

					if (tableName.equals("Configuration_")) {
						continue;
					}

					_moveCompanyData(companyId, tableName, statement);

					if (tableName.equals("DLFileEntryType")) {
						_moveData(companyId, false, tableName, statement, DLFILEENTRYTYPE_WHERECLAUSE);
					}
				}
			}

			System.out.println("Tables migrated");
		}
	}

	private static List<Long> _getCompanyIds() throws Exception {
		try (Statement statement = _connection.createStatement();
			 ResultSet resultSet = statement.executeQuery(
					 "select companyId from Company")) {

			List<Long> companyIds = new ArrayList<>();

			while (resultSet.next()) {
				companyIds.add(resultSet.getLong("companyId"));
			}

			return companyIds;
		}
	}

	private static String _getCreateTable(long companyId, String tableName) {
		return "create table if not exists " + _getSchemaName(companyId) +
				_PERIOD + tableName + " like " + _defaultSchemaName + _PERIOD +
				tableName;
	}

	private static String _getCreateView(long companyId, String viewName) {
		return "create or replace view " + _getSchemaName(companyId) + _PERIOD +
				viewName + " as select * from " + _defaultSchemaName + _PERIOD +
				viewName;
	}

	private static Long _getDefaultCompanyId() throws Exception {
		try (PreparedStatement preparedStatement =
			 _connection.prepareStatement(
				"select companyId from Company where webId = ?")) {

			preparedStatement.setString(1, _defaultWebId);

			try (ResultSet resultSet = preparedStatement.executeQuery()) {
				if (resultSet.next()) {
					return resultSet.getLong("companyId");
				}
			}
		}

		throw new Exception("Default company webId not found in database");
	}

	private static List<Long> _getNonDefaultCompanyIds() throws Exception {
		try (PreparedStatement preparedStatement = _connection.prepareStatement(
			"select companyId from Company where webId != ?")) {

			preparedStatement.setString(1, _defaultWebId);

			List<Long> companyIds = new ArrayList<>();

			try (ResultSet resultSet = preparedStatement.executeQuery()) {
				while (resultSet.next()) {
					companyIds.add(resultSet.getLong("companyId"));
				}
			}

			return companyIds;
		}
	}

	private static String _getSchemaName(long companyId) {
		if (companyId == _defaultCompanyId) {
			return _defaultSchemaName;
		}

		return _SCHEMA_PREFIX + companyId;
	}

	private static ScopeConfiguration _getScopeConfiguration(
			String configurationId, String dictionary)
		throws Exception {

		Dictionary<String, String> dictionaryMap = ConfigurationHandler.read(
			new ByteArrayInputStream(
				dictionary.getBytes("UTF-8")));

		Object value = dictionaryMap.get(
			ConfigurationProperties.Scope.COMPANY.getPropertyKey());

		if (value != null) {
			return new ScopeConfiguration(
				configurationId, dictionary, GetterUtil.getLong(value),
				ConfigurationProperties.Scope.COMPANY);
		}

		value = dictionaryMap.get(
			ConfigurationProperties.Scope.GROUP.getPropertyKey());

		if (value != null) {
			return new ScopeConfiguration(
				configurationId, dictionary, GetterUtil.getLong(value),
				ConfigurationProperties.Scope.GROUP);
		}

		value = dictionaryMap.get(
			ConfigurationProperties.Scope.PORTLET_INSTANCE.
				getPropertyKey());

		if (value != null) {
			return new ScopeConfiguration(
				configurationId, dictionary, GetterUtil.getString(value),
				ConfigurationProperties.Scope.PORTLET_INSTANCE);
		}

		return null;
	}

	private static boolean _hasColumn(String tableName, String columnName)
			throws Exception {

		DatabaseMetaData databaseMetaData = _connection.getMetaData();

		try (ResultSet rs = databaseMetaData.getColumns(
				_connection.getCatalog(), _connection.getSchema(), tableName,
				columnName)) {

			if (!rs.next()) {
				return false;
			}

			return true;
		}
	}

	private static void _insertConfiguration(Long companyId, ScopeConfiguration scopeConfiguration)
			throws Exception {

		try (PreparedStatement preparedStatement = _connection.prepareStatement(
				"insert into " + _getSchemaName(companyId) + _PERIOD + " Configuration_ " +
						"(configurationId, dictionary) values (?, ?)")) {

			preparedStatement.setString(1, scopeConfiguration.getConfigurationId());
			preparedStatement.setString(2, scopeConfiguration.getDictionary());

			preparedStatement.executeUpdate();
		}
	}

	private static boolean _isApplicable(
			long companyId, ScopeConfiguration scopeConfiguration)
		throws Exception {

		if (Objects.equals(
				scopeConfiguration.getScope(),
				ConfigurationProperties.Scope.COMPANY)) {

			if (companyId == (long)scopeConfiguration.getScopePK()) {
				return true;
			}

			return false;
		}

		if (Objects.equals(
				scopeConfiguration.getScope(),
				ConfigurationProperties.Scope.GROUP)) {

			try (PreparedStatement preparedStatement =
						 _connection.prepareStatement(
								 "select groupId from " + _getSchemaName(companyId) + _PERIOD + "Group_ where groupId = ?")) {

				preparedStatement.setLong(
						1, (long)scopeConfiguration.getScopePK());

				try (ResultSet resultSet = preparedStatement.executeQuery()) {
					if (resultSet.next()) {
						return true;
					}
				}
			}

			return false;
		}

		return true;
	}

	private static boolean _isControlTable(String tableName) {
		if (_controlTableNames.contains(tableName) ||
				tableName.startsWith("QUARTZ_")) {

			return true;
		}

		return false;
	}

	private static boolean _isObjectTable(List<Long> companyIds, String tableName) {
		for (long companyId : companyIds) {
			if (tableName.endsWith("_x_" + companyId) ||
					tableName.startsWith("L_" + companyId + "_") ||
					tableName.startsWith("O_" + companyId + "_")) {

				return true;
			}
		}

		return false;
	}

	private static void _moveCompanyData(
			long companyId, String tableName, Statement statement)
			throws Exception {

		String whereClause = "";

		if (_hasColumn(tableName, "companyId")) {
			whereClause = " where companyId = " + companyId;
		}

		_moveData(companyId, false, tableName, statement, whereClause);
	}

	private static void _moveConfigurationData(
			List<Long> companyIds)
			throws Exception {

		try (PreparedStatement preparedStatement =
					 _connection.prepareStatement(
							 "select configurationId, dictionary from " +
									 _defaultSchemaName + _PERIOD + "Configuration_");
			 ResultSet resultSet = preparedStatement.executeQuery()) {

			while (resultSet.next()) {
				ScopeConfiguration scopeConfiguration =
						_getScopeConfiguration(
								resultSet.getString(1), resultSet.getString(2));

				if (scopeConfiguration != null) {
					if (Objects.equals(
							scopeConfiguration.getScope(),
							ConfigurationProperties.Scope.PORTLET_INSTANCE)) {

						for (Long companyId : companyIds) {
							_insertConfiguration(companyId, scopeConfiguration);
						}

						continue;
					}

					if (!_isApplicable(_defaultCompanyId, scopeConfiguration)) {
						for (Long companyId : companyIds) {
							if (_isApplicable(companyId, scopeConfiguration)) {
								_insertConfiguration(companyId, scopeConfiguration);

								break;
							}
						}

						_removeConfiguration(scopeConfiguration);
					}
				}
			}
		}
	}

	private static void _moveData(
			long companyId, boolean removeTable, String tableName, Statement statement, String whereClause)
			throws Exception {

		statement.executeUpdate(
				"insert " + _getSchemaName(companyId) + _PERIOD + tableName +
						" select * from " + _defaultSchemaName + _PERIOD + tableName +
						whereClause);

		if (removeTable) {
			statement.executeUpdate(
					"drop table if exists " + _defaultSchemaName + _PERIOD +
							tableName);

			return;
		}

		if (!whereClause.isEmpty() && !whereClause.equals(DLFILEENTRYTYPE_WHERECLAUSE)) {
			statement.executeUpdate(
					"delete from " + _defaultSchemaName + _PERIOD + tableName +
							whereClause);
		}
	}

	private static void _removeConfiguration(ScopeConfiguration scopeConfiguration)
			throws Exception {

		try (PreparedStatement preparedStatement = _connection.prepareStatement(
				"delete from " + _defaultSchemaName + _PERIOD + " Configuration_ where configurationId = ?")) {

			preparedStatement.setString(1, scopeConfiguration.getConfigurationId());

			preparedStatement.executeUpdate();
		}
	}

	private static Connection _connection;

	private static Long _defaultCompanyId;

	private static String _defaultSchemaName;

	private static String _defaultWebId;

	private static final Set<String> _controlTableNames = new HashSet<>(
		Arrays.asList(
			"Company", "Release_", "ServiceComponent", "VirtualHost"));

	private static final String DLFILEENTRYTYPE_WHERECLAUSE = " where companyId = 0";

	private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
	private static final String JDBC_URL1 = "jdbc:mysql://localhost/";
	private static final String JDBC_URL2 =	"?characterEncoding=UTF-8&dontTrackOpenResources=true&holdResultsOpenOverStatementClose=true&serverTimezone=GMT&useFastDateParsing=false&useUnicode=true";

	private static final String _PERIOD = ".";

	private static final String _SCHEMA_PREFIX = "lpartition_";

	private static class ScopeConfiguration {

		public ScopeConfiguration(
				String configurationId, String dictionary, Serializable scopePK,
				ConfigurationProperties.Scope scope) {

			_configurationId = configurationId;
			_dictionary = dictionary;
			_scopePK = scopePK;
			_scope = scope;
		}

		public String getConfigurationId() {
			return _configurationId;
		}

		public String getDictionary() {
			return _dictionary;
		}

		public ConfigurationProperties.Scope getScope() {
			return _scope;
		}

		public Object getScopePK() {
			return _scopePK;
		}

		private final String _configurationId;
		private final String _dictionary;
		private final ConfigurationProperties.Scope _scope;
		private final Object _scopePK;

	}

}
