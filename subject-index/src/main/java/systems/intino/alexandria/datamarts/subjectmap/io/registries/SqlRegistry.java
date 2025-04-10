package systems.intino.alexandria.datamarts.subjectmap.io.registries;

import systems.intino.alexandria.datamarts.subjectmap.io.Registry;

import java.sql.*;
import java.util.*;
import java.util.function.IntPredicate;

public class SqlRegistry implements Registry {
	private final Connection connection;
	private final StatementProvider statementProvider;

	public SqlRegistry(Connection connection) {
		try {
			this.connection = connection;
			this.connection.setAutoCommit(false);
			this.initTables();
			this.statementProvider = new StatementProvider();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<String> subjects() {
		try (ResultSet rs = selectSubjects()) {
			return readStrings(rs);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<String> tokens() {
		try (ResultSet rs = selectTokens()) {
			return readStrings(rs);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void link(int subject, int token) {
		execute(subject, token, "link");
	}

	@Override
	public void unlink(int subject, int token) {
		execute(subject, token, "unlink");
	}

	@Override
	public List<Integer> tokensOf(int subject) {
		try (ResultSet rs = selectTokensOf(subject)) {
			return readIntegers(rs);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<Integer> subjectsFilteredBy(List<Integer> tokens) {
		try (ResultSet rs = selectSubjectsWith(tokens)) {
			return readIntegers(rs);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static List<String> readStrings(ResultSet rs) throws SQLException {
		List<String> result = new ArrayList<>();
		while (rs.next())
			result.add(rs.getString(1));
		return result;
	}

	private static List<Integer> readIntegers(ResultSet rs) throws SQLException {
		List<Integer> result = new ArrayList<>();
		while (rs.next())
			result.add(rs.getInt(1));
		return result;
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
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private ResultSet selectTokensOf(int subject) throws SQLException {
		PreparedStatement statement = statementProvider.get("get-subject-tokens");
		statement.setInt(1, subject);
		statement.executeQuery();
		return statement.executeQuery();
	}

	private ResultSet selectSubjectsWith(List<Integer> tokens) throws SQLException {
		String sql = sqlIn(tokens);
		PreparedStatement statement = connection.prepareStatement(sql);
		return statement.executeQuery();
	}

	private static final String SELECT_SUBJECTS = "SELECT DISTINCT subject_id FROM links";
	private static String sqlIn(List<Integer> tokens) {
		return (SELECT_SUBJECTS + exists(tokens) + notExists(tokens))
				.replace("links AND", "links WHERE");
	}

	private static String exists(List<Integer> tokens) {
		int[] items = positive(tokens);
		return items.length > 0 ? " AND subject_id IN (SELECT subject_id FROM links WHERE token_id IN (" + placeholders(items) + "))" : "";
	}

	private static String notExists(List<Integer> tokens) {
		int[] items = negative(tokens);
		return items.length > 0 ? " AND subject_id NOT IN (SELECT subject_id FROM links WHERE token_id IN (" + placeholders(items) + "))" : "";
	}

	private static int[] positive(List<Integer> tokens) {
		return filter(tokens, i -> i > 0);
	}

	private static int[] negative(List<Integer> tokens) {
		return filter(tokens, i -> i < 0);
	}

	private static int[] filter(List<Integer> tokens, IntPredicate filter) {
		return tokens.stream().mapToInt(i -> i).filter(filter).map(Math::abs).toArray();
	}

	private static String placeholders(int[] array) {
		return Arrays.toString(array).replace('[', '(').replace(']', ')');
	}

	private void execute(int subject, int token, String order) {
		try {
			PreparedStatement statement = statementProvider.get(order);
			statement.setInt(1, subject);
			statement.setInt(2, token);
			statement.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private ResultSet selectSubjects() throws SQLException {
		PreparedStatement statement = statementProvider.get("select-subject");
		return statement.executeQuery();
	}

	private ResultSet selectTokens() throws SQLException {
		PreparedStatement statement = statementProvider.get("select-tokens");
		return statement.executeQuery();
	}

	@Override
	public int insertSubject(String subject) {
		try {
			PreparedStatement statement = statementProvider.get("insert-subject");
			statement.setString(1, subject);
			statement.executeUpdate();
			ResultSet keys = statement.getGeneratedKeys();
			if (keys.next()) return keys.getInt(1);
			return -1;
		}
		catch (SQLException e) {
			throw new RuntimeException("Failed to insert subject");
		}
	}

	@Override
	public int insertToken(String token) {
		try {
			PreparedStatement statement = statementProvider.get("insert-token");
			statement.setString(1, token);
			statement.executeUpdate();
			ResultSet keys = statement.getGeneratedKeys();
			if (keys.next()) return keys.getInt(1);
			return -1;
		}
		catch (SQLException e) {
			throw new RuntimeException("Failed to insert token");
		}
	}

	@Override
	public void drop(int subject) {
		try {
			PreparedStatement statement = statementProvider.get("delete-subject");
			statement.setInt(1, subject);
			statement.executeUpdate();
			connection.commit();
		}
		catch (SQLException e) {
			throw new RuntimeException("Failed to drop subject");
		}
	}

	private class StatementProvider {

		final Map<String, PreparedStatement> statements;

		StatementProvider() throws SQLException {
			this.statements = statements();
		}

		Map<String, PreparedStatement> statements() throws SQLException {
			Map<String, PreparedStatement> statements = new HashMap<>();
			statements.put("select-subject", create("SELECT name FROM subject ORDER BY id"));
			statements.put("select-tokens", create("SELECT name FROM tokens ORDER BY id"));
			statements.put("get-subject-tokens", create("SELECT token_id FROM links WHERE subject_id = ?"));
			statements.put("insert-subject", create("INSERT INTO subject (name) VALUES (?)"));
			statements.put("insert-token", create("INSERT INTO tokens (name) VALUES (?)"));
			statements.put("delete-subject", create("UPDATE subject SET name = NULL WHERE id = ?"));
			statements.put("link", create("INSERT OR IGNORE INTO links (subject_id, token_id) VALUES (?, ?)"));
			statements.put("unlink", create("DELETE FROM links WHERE subject_id = ? and token_id = ?"));
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
			CREATE TABLE IF NOT EXISTS subject (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT
			);
			
			CREATE TABLE IF NOT EXISTS tokens (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name TEXT
			);
			
			CREATE TABLE IF NOT EXISTS links (
				subject_id INTEGER NOT NULL,
				token_id INTEGER NOT NULL,
				PRIMARY KEY (subject_id, token_id)
			);
			
			CREATE INDEX IF NOT EXISTS idx_subject ON links(subject_id);
			CREATE INDEX IF NOT EXISTS idx_token ON links(token_id);
			""";

	private void initTables() throws SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate(InitTables);
		connection.commit();
	}

}
