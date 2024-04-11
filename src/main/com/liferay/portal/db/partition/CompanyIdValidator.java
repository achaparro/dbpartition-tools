package main.com.liferay.portal.db.partition;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Alberto Chaparro
 */
public class CompanyIdValidator {

    public static void main(String[] args) throws Exception {
        Options options = _getOptions();

        if ((args.length != 0) &&
                (args[0].equals("-h") || args[0].endsWith("help"))) {

            HelpFormatter helpFormatter = new HelpFormatter();

            helpFormatter.printHelp(
                    "Liferay Portal Tools CompanyId Validator", options);

            return;
        }

        CommandLineParser commandLineParser = new DefaultParser();

        CommandLine commandLine = commandLineParser.parse(options, args);

        long companyId = Long.valueOf(commandLine.getOptionValue("companyId"));
        String jdbcURL = commandLine.getOptionValue("jdbc-url");
        String password = commandLine.getOptionValue("password");
        String user = commandLine.getOptionValue("user");

        _connection = DriverManager.getConnection(jdbcURL, user, password);

        _databaseMetaData = _connection.getMetaData();

        List<String> tableNames = _getTableNames();

        for (String tableName : tableNames) {
            String primaryKeyName = _getPrimaryKeyName(tableName);

            try (ResultSet resultSet = _databaseMetaData.getColumns(
                    _connection.getCatalog(), _connection.getSchema(),
                    tableName, null)) {

                while (resultSet.next()) {
                    int columnType = resultSet.getInt("DATA_TYPE");

                    if ((columnType != Types.LONGVARCHAR) && (columnType != Types.VARCHAR)) {
                        continue;
                    }

                    String columnName = resultSet.getString("COLUMN_NAME");

                    System.out.println("Checking " + tableName + "." + columnName);

                    PreparedStatement preparedStatement =
                        _connection.prepareStatement(
                            "select " + primaryKeyName + ", " + columnName + " from " + tableName + " where " + columnName + " like '%" + companyId + "%'");

                    try (ResultSet resultSet2 = preparedStatement.executeQuery()) {
                        while (resultSet2.next()) {
                            long primaryKey = resultSet2.getLong(primaryKeyName);
                            String column = resultSet2.getString(columnName);

                            System.out.println("Found record with companyId on it");
                            System.out.println(tableName);
                            System.out.println(primaryKeyName + ": " + primaryKey);
                            System.out.println(columnName + ": " + column);
                        }
                    }
                }
            }
        }
    }

    private static List<String> _getTableNames() throws Exception {

        List<String> tableNames = new ArrayList<>();

        try (ResultSet resultSet = _databaseMetaData.getTables(
                _connection.getCatalog(), _connection.getSchema(), null,
                new String[] {"TABLE"})) {

            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");

                if (_controlTableNames.contains(tableName.toLowerCase())) {
                    continue;
                }

                tableNames.add(tableName);
            }
        }

        return tableNames;
    }

    private static String _getPrimaryKeyName(String tableName) throws Exception {
        String primaryKeyName = "";

        try (ResultSet resultSet = _databaseMetaData.getPrimaryKeys(
                _connection.getCatalog(), _connection.getSchema(),
                tableName)) {

            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");

                if (columnName.equals("ctCollectionId") && !tableName.equals("CTCollection")) {
                    continue;
                }

                primaryKeyName = columnName;

                break;
            }
        }

        return primaryKeyName;
    }

    private static Options _getOptions() {
        Options options = new Options();

        options.addOption("a", "debug", false, "Print all log traces.");
        options.addOption("c", "companyId", true, "Set companyId");
        options.addOption("h", "help", false, "Print help message.");
        options.addOption("j", "jdbc-url", true, "Set the JDBC url.");
        options.addRequiredOption(
                "p", "password", true, "Set database user password.");
        options.addRequiredOption(
                "u", "user", true, "Set the database user name.");

        return options;
    }

    private static Connection _connection;

    private static DatabaseMetaData _databaseMetaData;

    private static final Set<String> _controlTableNames = new HashSet<>(
            Arrays.asList("classname_", "counter", "company", "virtualhost", "release_", "resourceaction", "servicecomponent"));

}

