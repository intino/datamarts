package systems.intino.alexandria.datamarts.anchormap.io.indexes;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import systems.intino.alexandria.datamarts.anchormap.io.Index;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqliteIndex implements Index {
	private final HikariDataSource dataSource;
	private final Connection connection;
	private final StatementProvider statementProvider;

	public SqliteIndex(File file) {
		try {
			this.dataSource = dataSourceOf(file);
			this.connection = dataSource.getConnection();
			this.connection.setAutoCommit(false);
			this.initTables();
			this.statementProvider = new StatementProvider();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static HikariDataSource dataSourceOf(File file) {
		return new HikariDataSource(configOf(file));
	}

	private static HikariConfig configOf(File file) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl("jdbc:sqlite:" + file.getAbsolutePath());
		config.setMaximumPoolSize(1); // SQLite is single-connection friendly
		config.setConnectionTestQuery("SELECT 1");
		config.setPoolName("sqlite-index-pool");
		config.setAutoCommit(false); // We'll manage manually
		return config;
	}

	@Override
	public int anchor(String name, String type) {
		try (ResultSet rs = selectAnchor(name, type)) {
			if (rs.next()) return rs.getInt("id");
			return insertAnchor(name, type);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int token(String value) {
		try (ResultSet rs = selectToken(value)) {
			if (rs.next()) return rs.getInt("id");
			return insertToken(value);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void push(int anchor, int token) {
		execute(anchor, token, "push");
	}

	@Override
	public void drop(int anchor, int token) {
		execute(anchor, token, "drop");
	}

	@Override
	public List<String> get(String anchor, String type) {
		try (ResultSet rs = select(anchor, type)) {
			List<String> result = new ArrayList<>();
			while (rs.next())
				result.add(rs.getString("token"));
			return result;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}	}

	@Override
	public List<String> search(String type, List<String> tokens) {
		try (ResultSet rs = select(type, tokens)) {
			List<String> result = new ArrayList<>();
			while (rs.next())
				result.add(rs.getString("name") + ":" +rs.getString("type"));
			return result;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void commit() {
		try {
			connection.commit();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		try {
			connection.close();
			dataSource.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private ResultSet select(String anchor, String type) {
		try {
			PreparedStatement statement = statementProvider.get("get-anchor-tokens");
			statement.setString(1, anchor);
			statement.setString(2, type);
			statement.executeQuery();
			return statement.executeQuery();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("SqlSourceToSinkFlow")
	private ResultSet select(String type, List<String> tokens) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sqlIn(tokens));
		statement.setString(1, type);
		return statement.executeQuery();
	}

	private static String sqlIn(List<String> tokens) {
		return new QueryBuilder().add(tokens).toString();
	}

	private void execute(int anchor, int token, String order) {
		try {
			PreparedStatement statement = statementProvider.get(order);
			statement.setInt(1, anchor);
			statement.setInt(2, token);
			statement.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private ResultSet selectAnchor(String name, String type) throws SQLException {
		PreparedStatement statement = statementProvider.get("get-anchor");
		statement.setString(1, name);
		statement.setString(2, type);
		return statement.executeQuery();
	}

	private int insertAnchor(String name, String type) throws SQLException {
		PreparedStatement statement = statementProvider.get("add-anchor");
		statement.setString(1, name);
		statement.setString(2, type);
		statement.executeUpdate();
		ResultSet keys = statement.getGeneratedKeys();
		if (keys.next()) return keys.getInt(1);
		throw new SQLException("Failed to insert anchor");
	}

	private ResultSet selectToken(String value) throws SQLException {
		PreparedStatement statement = statementProvider.get("get-token");
		statement.setString(1, value);
		return statement.executeQuery();
	}

	private int insertToken(String value) throws SQLException {
		PreparedStatement statement = statementProvider.get("add-token");
		statement.setString(1, value);
		statement.executeUpdate();
		ResultSet keys = statement.getGeneratedKeys();
		if (keys.next()) return keys.getInt(1);
		throw new SQLException("Failed to insert token");
	}

	private class StatementProvider {

		final Map<String, PreparedStatement> statements;

		StatementProvider() throws SQLException {
			this.statements = statements();
		}

		Map<String, PreparedStatement> statements() throws SQLException {
			Map<String, PreparedStatement> statements = new HashMap<>();
			statements.put("get-anchor", create("SELECT id FROM anchors WHERE name = ? AND type = ?"));
			statements.put("get-token", create("SELECT id FROM tokens WHERE token = ?"));
			statements.put("get-anchor-tokens", create("SELECT t.token FROM tokens t JOIN inverted_index i ON t.id = i.token_id JOIN anchors a ON a.id = i.anchor_id WHERE a.name = ? AND a.type = ?"));
			statements.put("add-anchor", create("INSERT INTO anchors (name, type) VALUES (?, ?)"));
			statements.put("add-token", create("INSERT INTO tokens (token) VALUES (?)"));
			statements.put("push", create("INSERT OR IGNORE INTO inverted_index (anchor_id, token_id) VALUES (?, ?)"));
			statements.put("drop", create("DELETE FROM inverted_index WHERE anchor_id = ? and token_id = ?"));
			return statements;
		}

		PreparedStatement get(String sql) {
			return statements.get(sql);
		}

		private PreparedStatement create(String sql) throws SQLException {
			return connection.prepareStatement(sql);
		}

	}

	private static final String InitTables = """
			CREATE TABLE IF NOT EXISTS anchors (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT NOT NULL,
				type TEXT NOT NULL,
				UNIQUE(name, type)
			);
			
			CREATE TABLE IF NOT EXISTS tokens (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				token TEXT NOT NULL UNIQUE
			);
			
			CREATE TABLE IF NOT EXISTS inverted_index (
				anchor_id INTEGER NOT NULL,
				token_id INTEGER NOT NULL,
				PRIMARY KEY (token_id, anchor_id)
			);
			
			CREATE INDEX IF NOT EXISTS idx_anchors_type ON anchors(type);
			CREATE INDEX IF NOT EXISTS idx_inverted_anchor ON inverted_index(anchor_id);
			CREATE INDEX IF NOT EXISTS idx_inverted_token ON inverted_index(token_id);
			""";

	private void initTables() throws SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate(InitTables);
		connection.commit();
	}

	public static class QueryBuilder {
		private final StringBuilder sb = new StringBuilder();

		public QueryBuilder() {
			this.sb.append(" SELECT a.name, a.type FROM anchors a WHERE a.type = ?");
		}

		public QueryBuilder add(List<String> tokens) {
			tokens.forEach(this::add);
			return this;
		}

		@Override
		public String toString() {
			return sb.toString();
		}

		private void add(String token) {
			this.sb.append(" AND ")
					.append(token.charAt(0) == '+' ? "" : "NOT ")
					.append("EXISTS (")
					.append("SELECT 1 FROM inverted_index i JOIN tokens t ON t.id = i.token_id WHERE i.anchor_id = a.id AND t.token = ")
					.append(quote(token.substring(1)))
					.append(")");
		}

		private static String quote(String type) {
			return "'" + type + "'";
		}
	}
}
