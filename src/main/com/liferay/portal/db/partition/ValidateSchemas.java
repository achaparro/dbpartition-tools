/**
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
 
package main.com.liferay.portal.db.partition;

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
public class ValidateSchemas {

	public static void main(String[] args) throws Exception {
		System.out.println("*** Start validating DB Partition schemas ***");

		if (args.length != 3 && args.length != 4) {
			System.out.println("Database name, user and password are required");
			System.out.println("Optionally, add the schema prefix if changed");

			return;
		}

		// Todo: remove companyId option, support schema prefix

		if (args.length == 4) {
			_schema_prefix = args[3];
		}

		_schemaName = args[0];

		Class.forName(JDBC_DRIVER).newInstance();

		try {
			_connection = DriverManager.getConnection(
				JDBC_URL1 + _schemaName + JDBC_URL2, args[1], args[2]);

			boolean defaultSchema = true;

			for (Long companyId : _getCompanies()) {
				if (defaultSchema) {
					_validateSchema(companyId, _schemaName, true);

					defaultSchema = false;

					continue;
				}

				String schemaName = _schema_prefix + companyId;

				_validateSchema(companyId, schemaName, false);
			}
		}
		finally {
			if (_connection != null) {
				_connection.close();
			}
		}

		System.out.println("*** End validating DB Partition schemas ***");
	}

	private static List<Long> _getCompanies() throws Exception {
		try (Statement statement = _connection.createStatement();
		     ResultSet resultSet = statement.executeQuery(
			     "select companyId from Company order by companyId asc")) {

			List<Long> companyIds = new ArrayList<>();

			while (resultSet.next()) {
				companyIds.add(resultSet.getLong("companyId"));
			}

			return companyIds;
		}
	}

	private static void _validateSchema(
			Long companyId, String schemaName, boolean defaultSchema)
		throws Exception {

		System.out.println("** Validating schema " + schemaName);

		DatabaseMetaData databaseMetaData = _connection.getMetaData();

		try (ResultSet resultSet = databaseMetaData.getTables(
				schemaName, schemaName, null, new String[]{"TABLE"})) {

			while (resultSet.next()) {
				String tableName = resultSet.getString("TABLE_NAME");

				if (_controlTableNames.contains(tableName)) {
					continue;
				}

				if (_hasColumn(schemaName, tableName, "companyId")) {
					String fullTableName = schemaName + _PERIOD + tableName;

					String query =
						"select count(*) from " + fullTableName + " where " +
							"companyId != " + companyId;

					if (defaultSchema) {
						query += " and companyId != 0";
					}

					try (PreparedStatement preparedStatement =
						     _connection.prepareStatement(query);
						ResultSet resultSet1 =
							preparedStatement.executeQuery()) {

						if (resultSet1.next()) {
							int count = resultSet1.getInt(1);

							if (count > 0) {
								System.out.println(
									"Error: Table " + fullTableName +
										" contains " + resultSet1.getInt(1) +
										" records with an invalid companyId");
							}
						}
					}
				}
			}
		}
	}

	private static boolean _hasColumn(
		String schemaName, String tableName, String columnName)
		throws Exception {

		DatabaseMetaData databaseMetaData = _connection.getMetaData();

		try (ResultSet rs = databaseMetaData.getColumns(
			schemaName, schemaName, tableName, columnName)) {

			if (!rs.next()) {
				return false;
			}

			return true;
		}
	}

	private static Connection _connection;

	private static String _schemaName;

	private static String _schema_prefix = "lpartition_";

	private static final Set<String> _controlTableNames = new HashSet<>(
		Arrays.asList("Company", "VirtualHost"));

	private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
	private static final String JDBC_URL1 = "jdbc:mysql://localhost/";
	private static final String JDBC_URL2 =	"?characterEncoding=UTF-8&dontTrackOpenResources=true&holdResultsOpenOverStatementClose=true&serverTimezone=GMT&useFastDateParsing=false&useUnicode=true";

	private static final String _PERIOD = ".";

}
