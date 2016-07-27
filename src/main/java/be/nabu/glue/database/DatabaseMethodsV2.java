package be.nabu.glue.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import be.nabu.glue.annotations.GlueMethod;
import be.nabu.glue.annotations.GlueParam;
import be.nabu.libs.evaluator.annotations.MethodProviderClass;

@MethodProviderClass(namespace = "database")
public class DatabaseMethodsV2 {

	@GlueMethod(description = "Run an insert/update/delete statement on the database", version = 2)
	public static Iterable<?> update(@GlueParam(name = "sql") String sql, @GlueParam(name = "database") String database) throws SQLException, IOException {
		List<Object> values = new ArrayList<Object>();
		Connection connection = DatabaseMethods.getConnection(database);
		try {
			PreparedStatement preparedStatement = DatabaseMethods.prepare(connection, sql, database);
			values.add(preparedStatement.executeUpdate());
			ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
			while (generatedKeys.next()) {
				values.add(generatedKeys.getLong(1));
			}
			return values;
		}
		finally {
			connection.close();
		}
	}
	
	@GlueMethod(description = "Run a select on the database", version = 2)
	public static Iterable<?> select(@GlueParam(name = "sql") String sql, @GlueParam(name = "database") String database) throws SQLException, IOException {
		Connection connection = DatabaseMethods.getConnection(database);
		try {
			PreparedStatement preparedStatement = DatabaseMethods.prepare(connection, sql, database);
			ResultSet result = preparedStatement.executeQuery();
			ResultSetMetaData metadata = result.getMetaData();
			int amountOfColumns = metadata.getColumnCount();
			List<List<?>> rows = new ArrayList<List<?>>();
			while(result.next()) {
				List<Object> row = new ArrayList<Object>();
				for (int i = 1; i <= amountOfColumns; i++) {
					row.add(result.getObject(i));
				}
				rows.add(row);
			}
			return rows;
		}
		finally {
			connection.close();
		}
	}
	
}
