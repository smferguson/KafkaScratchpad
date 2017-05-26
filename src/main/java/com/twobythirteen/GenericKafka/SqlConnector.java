package com.twobythirteen.GenericKafka;


import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class SqlConnector {
	static final Logger LOGGER = LoggerFactory.getLogger(SqlConnector.class);

	private Connection connection;
	private PreparedStatement preparedStatement;

	public SqlConnector(String connectionString, String user, String password) throws SQLException {
		this.connection = DriverManager.getConnection(connectionString, user, password);
	}

	public String getPrimaryKeyColumn(String tableSchema, String tableName) throws SQLException {
		String sql = "SELECT column_name\n" +
				"FROM information_schema.columns\n" +
				"WHERE table_name = \"" + tableName + "\"\n" +
				"AND table_schema = \"" + tableSchema + "\"\n" +
				"AND column_key = \"PRI\"";

		PreparedStatement primaryKeyQuery = connection.prepareStatement(sql);
		ResultSet rs = primaryKeyQuery.executeQuery();
		String primaryKey = null;
		while (rs.next()) {
			primaryKey = rs.getString("column_name");
		}
		return primaryKey;
	}

	public ArrayList<String> getColumnsForTable(String tableSchema, String tableName) throws SQLException {
		String sql = "SELECT column_name\n" +
				"FROM information_schema.columns\n" +
				"WHERE table_name = \"" + tableName + "\"\n" +
				"AND table_schema = \"\"" + tableSchema + "\"\"";

		ArrayList<String> columns = new ArrayList<String>();

		PreparedStatement columnsQuery = connection.prepareStatement(sql);
		ResultSet rs = columnsQuery.executeQuery();
		while (rs.next()) {
			columns.add(rs.getString("column_name"));
		}
		return columns;
	}

	public HashMap<String, JSONArray> extractTable(String tableSchema, String table) throws SQLException {
		String primaryKeyColumn = getPrimaryKeyColumn(tableSchema, table);
		ArrayList<String> columns = getColumnsForTable(tableSchema, table);

		String query = "SELECT * FROM " + table;
		preparedStatement = connection.prepareStatement(query);
		ResultSet rs = preparedStatement.executeQuery();
		HashMap<String, JSONArray> tableContents = new HashMap<String, JSONArray>();
		JSONArray data = new JSONArray();
		while (rs.next()) {
			JSONObject oneRow = new JSONObject();
			for (String column : columns) {
				oneRow.put(column, rs.getString(column));
			}
			data.put(oneRow);
		}

		tableContents.put(primaryKeyColumn, data);
		return tableContents;
	}

	private HashMap<String, JSONObject> convertToMap(String primaryKeyColumnName, JSONArray contents) {
		HashMap<String, JSONObject> map = new HashMap<String, JSONObject>();

		for (Object content : contents) {
			JSONObject c = (JSONObject) content;
			map.put(c.getJSONObject("data").get(primaryKeyColumnName).toString(), c.getJSONObject("data"));
		}

		return map;
	}

	public void compareTableToKafka(String tableName, HashMap<String, JSONArray> tableContents, JSONArray kafkaContents) {
		HashMap<String, JSONObject> kafkaMap = null;
		for (String primaryKeyColumnName : tableContents.keySet()) {
			int recordsPassed = 0;
			for (Object oneRow : tableContents.get(primaryKeyColumnName)) {
				JSONObject tableRow = (JSONObject) oneRow;
				String primaryKeyValue = tableRow.getString(primaryKeyColumnName);

				if (kafkaMap == null) {
					kafkaMap = convertToMap(primaryKeyColumnName, kafkaContents);
				}

				JSONObject kafkaRow = kafkaMap.get(primaryKeyValue);
				for (String column : tableRow.keySet()) {
					try {
						if (!tableRow.get(column).toString().equalsIgnoreCase(kafkaRow.get(column).toString())) {
							LOGGER.info("*********************");
							LOGGER.info("Row with " + primaryKeyColumnName + " " + primaryKeyValue + " failed on column " + column);
							LOGGER.info("Table value: " + tableRow.get(column).toString());
							LOGGER.info("Kafka value: " + kafkaRow.get(column).toString());
							LOGGER.info(tableRow.toString());
							LOGGER.info(kafkaRow.toString());
							LOGGER.info("*********************");
						} else {
							recordsPassed++;
						}
					} catch (Exception je) {
						LOGGER.info("Exception processiong row with " + primaryKeyColumnName + " " + primaryKeyValue + " on column " + column);
						LOGGER.info(tableRow.toString());
						LOGGER.info(kafkaRow.toString());
						je.printStackTrace();
						throw new RuntimeException("^^ Exception processing data.");
					}
				}
			}
			LOGGER.info(tableName + " database data matched Kafka data. " + recordsPassed + " passed.");
		}
	}
}
