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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Alberto Chaparro
 */
public class MigrateToDBPartition {

	public static void main(String[] args) throws Exception {
		System.out.println("*** Start migrating companies to DB Partition ***");

		if (args.length != 3) {
			System.out.println("Database name, user and password are required");

			return;
		}

		_defaultSchemaName = args[0];

		Class.forName(JDBC_DRIVER).newInstance();

		try {
			_connection = DriverManager.getConnection(
				JDBC_URL1 + _defaultSchemaName + JDBC_URL2, args[1], args[2]);

			List<Long> companyIds = _getNonDefaultCompanyIds();

			for (Long companyId : companyIds) {
				System.out.println("** Migrating company with id " + companyId);

				_createSchema(companyId);
			}
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

			try (ResultSet resultSet = databaseMetaData.getTables(
				_connection.getCatalog(), _connection.getSchema(), null,
				new String[]{"TABLE"});
				 Statement statement = _connection.createStatement()) {

				while (resultSet.next()) {
					String tableName = resultSet.getString("TABLE_NAME");

					if (_isControlTable(tableName)) {
						statement.executeUpdate(
							_getCreateView(companyId, tableName));
					}
					else {
						statement.executeUpdate(
							_getCreateTable(companyId, tableName));

						_moveCompanyData(companyId, tableName, statement);
					}
				}
			}

			System.out.println("Tables migrated");
		}
	}

	private static void _moveCompanyData(
		long companyId, String tableName, Statement statement)
		throws Exception {

		String whereClause = " where companyId = " + companyId;

		statement.executeUpdate(
			"insert " + _getSchemaName(companyId) + _PERIOD + tableName +
			" select * from " + _defaultSchemaName + _PERIOD + tableName +
			whereClause);

		if (!whereClause.isEmpty()) {
			statement.executeUpdate(
				"delete from " + _defaultSchemaName + _PERIOD + tableName +
					whereClause);
		}
	}


	private static boolean _isControlTable(String tableName) throws Exception {

		if (_controlTableNames.contains(tableName) ||
			tableName.startsWith("QUARTZ_") ||
			!hasColumn(tableName, "companyId")) {

			return true;
		}

		return false;
	}

	private static boolean hasColumn(String tableName, String columnName)
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

	private static String _getSchemaName(long companyId) {
		return _SCHEMA_PREFIX + companyId;
	}

	private static String _getCreateView(long companyId, String viewName) {
		return "create or replace view " + _getSchemaName(companyId) + _PERIOD +
		   viewName + " as select * from " + _defaultSchemaName + _PERIOD +
		   viewName;
	}

	private static String _getCreateTable(long companyId, String tableName) {
		return "create table if not exists " + _getSchemaName(companyId) +
			_PERIOD + tableName + " like " + _defaultSchemaName + _PERIOD +
			tableName;
	}

	private static List<Long> _getNonDefaultCompanyIds() throws Exception {
		try (Statement statement = _connection.createStatement();
			 ResultSet resultSet = statement.executeQuery(
			 	"select companyId from Company where companyId >" +
					"(select min(companyId) from Company)")
			) {

			List<Long> companyIds = new ArrayList<>();

			while (resultSet.next()) {
				companyIds.add(resultSet.getLong("companyId"));
			}

			return companyIds;
		}
	}

	private static Connection _connection;

	private static String _defaultSchemaName;

	private static final Set<String> _controlTableNames = new HashSet<>(
		Arrays.asList("Company", "Portlet", "VirtualHost"));

	private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
	private static final String JDBC_URL1 = "jdbc:mysql://localhost/";
	private static final String JDBC_URL2 =	"?characterEncoding=UTF-8&dontTrackOpenResources=true&holdResultsOpenOverStatementClose=true&serverTimezone=GMT&useFastDateParsing=false&useUnicode=true";

	private static final String _PERIOD = ".";

	private static final String _SCHEMA_PREFIX = "lpartition_";
}
