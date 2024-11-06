/*
* Copyright (C) 2015 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.glue.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import be.nabu.glue.annotations.GlueMethod;
import be.nabu.glue.annotations.GlueParam;
import be.nabu.glue.api.ExecutionException;
import be.nabu.glue.core.impl.methods.ScriptMethods;
import be.nabu.glue.utils.ScriptRuntime;
import be.nabu.glue.utils.VirtualScript;
import be.nabu.libs.evaluator.annotations.MethodProviderClass;

@MethodProviderClass(namespace = "database")
public class DatabaseMethods {
	
	private static Map<String, DataSource> datasources = new HashMap<String, DataSource>();
	
	static DataSource getDataSource(String name) {
		String environment = ScriptRuntime.getRuntime().getExecutionContext().getExecutionEnvironment().getName();
		if (!datasources.containsKey(environment + "." + name)) {
			synchronized(datasources) {
				if (!datasources.containsKey(environment + "." + name)) {
					Properties properties = new Properties();
					properties.put("driverClassName", name == null ? ScriptMethods.environment("database.driver") : ScriptMethods.environment("database." + name + ".driver"));
					properties.put("jdbcUrl", name == null ? ScriptMethods.environment("database.jdbcUrl") : ScriptMethods.environment("database." + name + ".jdbcUrl"));
					String userName = name == null ? ScriptMethods.environment("database.username") : ScriptMethods.environment("database." + name + ".username");
					if (userName != null) {
						properties.put("username", userName);
					}
					String password = name == null ? ScriptMethods.environment("database.password") : ScriptMethods.environment("database." + name + ".password");
					if (password != null) {
						properties.put("password", password);
					}
					properties.put("autoCommit", "true");
					HikariDataSource datasource = new HikariDataSource(new HikariConfig(properties));
					datasources.put(environment + "." + name, datasource);
				}
			}
		}
		return datasources.get(environment + "." + name);
	}
	
	static Connection getConnection(String name) throws SQLException {
		return getDataSource(name).getConnection();
	}
	
	@GlueMethod(version = 1)
	public static Object[] sql(String sql) throws SQLException, IOException {
		return sql(sql, null);
	}
	
	@GlueMethod(description = "Define a new datasource at runtime")
	public static void datasource(@GlueParam(name = "name", description = "The name of the datasource") String name, @GlueParam(name = "driver") String driver, @GlueParam(name = "jdbcUrl") String jdbcUrl, @GlueParam(name = "userName") String userName, @GlueParam(name = "password") String password) {
		String environment = ScriptRuntime.getRuntime().getExecutionContext().getExecutionEnvironment().getName();
		if (!datasources.containsKey(environment + "." + name)) {
			synchronized(datasources) {
				if (!datasources.containsKey(environment + "." + name)) {
					Properties properties = new Properties();
					properties.put("driverClassName", driver);
					properties.put("jdbcUrl", jdbcUrl);
					if (userName != null) {
						properties.put("username", userName);
					}
					if (password != null) {
						properties.put("password", password);
					}
					properties.put("autoCommit", "true");
					HikariDataSource datasource = new HikariDataSource(new HikariConfig(properties));
					datasources.put(environment + "." + name, datasource);
				}
			}
		}
	}
	
	@GlueMethod(description = "Run an sql on a database", version = 1)
	public static Object[] sql(@GlueParam(name = "sql") String sql, @GlueParam(name = "database") String database) throws SQLException, IOException {
		// get the first keyword from the sql, it can be select or 
		String keyword = sql.replaceAll("^[^\\w]*([\\w]+).*$", "$1");
		if (keyword.equalsIgnoreCase("select")) {
			return select(sql, database);
		}
		else {
			// the update does not return a matrix but a single row
			// make it a matrix for consistent return values
			Object[] matrix = new Object[1];
			matrix[0] = update(sql, database);
			return matrix;
		}
	}

	@GlueMethod(description = "Run an insert/update/delete statement on the database", version = 1)
	public static Object[] update(@GlueParam(name = "sql") String sql, @GlueParam(name = "database") String database) throws SQLException, IOException {
		List<Object> values = new ArrayList<Object>();
		Connection connection = getConnection(database);
		try {
			PreparedStatement preparedStatement = prepare(connection, sql, database);
			values.add(preparedStatement.executeUpdate());
			ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
			while (generatedKeys.next()) {
				values.add(generatedKeys.getLong(1));
			}
			return values.toArray();
		}
		finally {
			connection.close();
		}
	}
	
	@GlueMethod(description = "Run a select on the database", version = 1)
	public static Object[] select(@GlueParam(name = "sql") String sql, @GlueParam(name = "database") String database) throws SQLException, IOException {
		Connection connection = getConnection(database);
		try {
			PreparedStatement preparedStatement = prepare(connection, sql, database);
			ResultSet result = preparedStatement.executeQuery();
			ResultSetMetaData metadata = result.getMetaData();
			int amountOfColumns = metadata.getColumnCount();
			List<Object[]> rows = new ArrayList<Object[]>();
			while(result.next()) {
				List<Object> row = new ArrayList<Object>();
				for (int i = 1; i <= amountOfColumns; i++) {
					row.add(result.getObject(i));
				}
				rows.add(row.toArray());
			}
			return rows.toArray();
		}
		finally {
			connection.close();
		}
	}

	static PreparedStatement prepare(Connection connection, String sql, String database) throws SQLException, IOException {
		String regex = "(?<!:):[\\w]+";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(sql);
		List<String> inputNames = new ArrayList<String>();
		while (matcher.find()) {
			inputNames.add(matcher.group().substring(1));
		}
		PreparedStatement preparedStatement = connection.prepareStatement(ScriptMethods.string(sql.replaceAll(regex, "?"), true));
		Map<String, Object> pipeline = ScriptRuntime.getRuntime().getExecutionContext().getPipeline();
		for (int i = 0; i < inputNames.size(); i++) {
			if (pipeline.get(inputNames.get(i)) == null) {
				preparedStatement.setNull(i + 1, Types.VARCHAR);
			}
			else if (pipeline.get(inputNames.get(i)) instanceof Date) {
				preparedStatement.setTimestamp(i + 1, new java.sql.Timestamp(((Date) pipeline.get(inputNames.get(i))).getTime()));
			}
			else {
				preparedStatement.setObject(i + 1, pipeline.get(inputNames.get(i)));
			}
		}
		return preparedStatement;
	}
	
	@GlueMethod(description = "Run a select statement on the database and validate the results")
	public static void validateSql(@GlueParam(name = "description") String description, @GlueParam(name = "sql") String sql, @GlueParam(name = "expected") Object...expected) throws ExecutionException, IOException, ParseException {
		checkSql(false, description, sql, expected);
	}
	
	@GlueMethod(description = "Run a select statement on the database and validate the results")
	public static void confirmSql(@GlueParam(name = "description") String description, @GlueParam(name = "sql") String sql, @GlueParam(name = "expected") Object...expected) throws ExecutionException, IOException, ParseException {
		checkSql(true, description, sql, expected);
	}
	
	private static void checkSql(boolean fail, String description, String sql, Object...expected) throws ExecutionException, IOException, ParseException {
		sql = ScriptMethods.string(sql, true);
		StringBuilder builder = new StringBuilder();
		String method = fail ? "confirm" : "validate";
		builder.append("actual = database.select(\"" + sql + "\")\n");
		List<Object[]> expectedResults = new ArrayList<Object[]>();
		if (expected == null) {
			builder.append("@group = " + description + "\n");
			builder.append(method + "Null('Result must be null', actual)\n");
		}
		else {
			builder.append("@group = " + description + "\n");
			builder.append(method + "NotNull('Result must not be null', actual)\n");
			builder.append("@group = " + description + "\n");
			builder.append(method + "Equals('Result size check', " + expected.length + ", size(actual))\n");
			int rowCounter = 0;
			List<String> fields = getFields(sql);
			for (Object row : expected) {
				Object [] results;
				if (row instanceof Object[]) {
					results = (Object[]) row;
				}
				else if (row instanceof List) {
					results = ((List<?>) row).toArray();
				}
				else if (row instanceof Map) {
					results = ((Map<?, ?>) row).values().toArray();
				}
				else {
					results = new Object [] { row };
				}
				builder.append("@group = " + description + " row " + rowCounter + "\n");
				builder.append(method + "Equals('Size check', " + results.length + ", size(actual[" + rowCounter + "]))\n");
				expectedResults.add(results);
				for (int i = 0; i < results.length; i++) {
					String columnName = i < fields.size() ? " " + fields.get(i) : "";
					columnName = columnName.replaceAll("[^\\w\\s]+", "_");
					// do an explicit null check
					if (results[i] == null) {
						builder.append("@group = " + description + " row " + rowCounter + "\n");
						builder.append(method + "Null('Column " + i + columnName + "', actual[" + rowCounter + "][" + i + "])\n");
					}
					// if anything goes, just do a notnull check
					else if ("*".equals(results[i])) {
						builder.append("@group = " + description + " row " + rowCounter + "\n");
						builder.append(method + "NotNull('Column " + i + columnName + "', actual[" + rowCounter + "][" + i + "])\n");
					}
					else {
						builder.append("@group = " + description + " row " + rowCounter + "\n");
						builder.append(method + "Equals('Column " + i + columnName + "', expected[" + rowCounter + "][" + i + "] , actual[" + rowCounter + "][" + i + "])\n");
					}
				}
				rowCounter++;
			}
		}
		ScriptRuntime runtime = ScriptRuntime.getRuntime();
		ScriptMethods.debug("Generated glue script: " + builder.toString());
		ScriptRuntime forked = ScriptRuntime.getRuntime().fork(new VirtualScript(runtime.getScript(), builder.toString()), true);
		forked.getExecutionContext().getPipeline().put("expected", expectedResults);
		forked.run();
	}
	
	private static List<String> getFields(String sql) {
		List<String> fields = new ArrayList<String>();
		int indexOf = sql.indexOf("from");
		if (sql.toLowerCase().trim().startsWith("select") && indexOf >= 0) {
			sql = sql.trim().substring("select".length(), indexOf);
			for (String field : sql.split(",")) {
				indexOf = field.indexOf(" as ");
				if (indexOf >= 0) {
					field = field.substring(indexOf + " as ".length());
				}
				fields.add(field.trim());
			}
		}
		return fields;
	}
}
