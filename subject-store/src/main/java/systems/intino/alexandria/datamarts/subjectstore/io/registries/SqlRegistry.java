package systems.intino.alexandria.datamarts.subjectstore.io.registries;

import systems.intino.alexandria.datamarts.subjectstore.SubjectStore.RegistryException;
import systems.intino.alexandria.datamarts.subjectstore.io.Bundle;
import systems.intino.alexandria.datamarts.subjectstore.io.Registry;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.*;
import static systems.intino.alexandria.datamarts.subjectstore.model.TemporalReferences.BigBang;
import static systems.intino.alexandria.datamarts.subjectstore.model.TemporalReferences.Legacy;

public class SqlRegistry implements Registry {
	private final Connection connection;
	private final StatementProvider statementProvider;
	private final String id;
	private int feedCount;
	private int current;


	public SqlRegistry(String id, Connection connection) {
		try {
			this.id = id;
			this.connection = connection;
			this.connection.setAutoCommit(false);
			this.initTables();
			this.statementProvider = new StatementProvider();
			this.initTags();
			this.feedCount = readFeedCount();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int size() {
		return feedCount;
	}

	public Bundle tags() {
		try {
			return bundleOf(selectTags());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public Bundle instants() {
		try {
			return bundleOf(selectInstants());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String ss(int feed) {
		try {
			try (ResultSet select = selectTextValue(1, feed)) {
				return select.getString(1);
			}
		} catch (SQLException e) {
			throw new RegistryException(e);
		}
	}

	@Override
	public double getNumber(int tag, int feed)  {
		try (ResultSet select = selectDoubleValue(tag, feed)) {
			return select.getDouble(1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Bundle getNumbers(int tag, int from, int to) {
		try {
			return bundleOf(selectDoubleValues(tag, from, to));
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getText(int tag, int feed)  {
		try (ResultSet select = selectTextValue(tag, feed)) {
			return select.getString(1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Bundle getTexts(int tag, int from, int to) {
		try {
			return bundleOf(selectStringValues(tag, from, to));
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setTag(int id, String label) {
		try {
			insertTag(id, label);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setTagLastFeed(int id, int feed) {
		try {
			updateTag(id, feed);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public int nextFeed() {
		current = feedCount++;
		return current;
	}

	@Override
	public void put(int tag, Object o) {
		if (current < 0) return;
		try {
			switch (type(o)) {
				case NUMERIC -> insertEntry(tag, current, ((Number) o).doubleValue());
				case DATE -> insertEntry(tag, current, ((Instant) o));
				case VARCHAR -> insertEntry(tag, current, o.toString());
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void push() {
		try {
			updateSize(feedCount);
			current = -1;
			connection.commit();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void dump(OutputStream os, Map<Object, String> dictionary) {
		try {
			SqlDumper dumper = new SqlDumper(selectAll(), dictionary);
			dumper.execute(os);
		} catch (SQLException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static final int FEEDS = -1;

	private int readFeedCount() throws SQLException {
		return (int) getNumber(FEEDS, FEEDS);
	}

	private int type(Object o) {
		if (o instanceof Number) return NUMERIC;
		if (o instanceof Instant) return DATE;
		return VARCHAR;
	}

	private ResultSet selectAll() throws SQLException {
		return statementProvider.get("select-all").executeQuery();
	}

	private void initTables() throws SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate(InitTables.replace("[id]", id));
		connection.commit();
	}

	private void initTags() throws SQLException {
		try (ResultSet rs = selectTags()) {
			if (rs.next()) return;
			insertTag(0, "ts");
			insertTag(1, "ss");
			connection.commit();
		}
	}

	private ResultSet selectTags() throws SQLException {
		return statementProvider.get("select-tags").executeQuery();
	}

	private ResultSet selectInstants() throws SQLException {
		return statementProvider.get("select-instants").executeQuery();
	}

	private ResultSet selectDoubleValue(int tag, int feed) throws SQLException {
		PreparedStatement statement = statementProvider.get("select-double-value");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		return statement.executeQuery();
	}

	private ResultSet selectTextValue(int tag, int feed) throws SQLException {
		PreparedStatement statement = statementProvider.get("select-string-value");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		return statement.executeQuery();
	}

	private ResultSet selectDoubleValues(int tag, int from, int to) throws SQLException {
		PreparedStatement statement = statementProvider.get("select-double-values");
		statement.setInt(1, tag);
		statement.setInt(2, from);
		statement.setInt(3, to);
		return statement.executeQuery();
	}

	private ResultSet selectStringValues(int tag, int from, int to) throws SQLException {
		PreparedStatement statement = statementProvider.get("select-string-values");
		statement.setInt(1, tag);
		statement.setInt(2, from);
		statement.setInt(3, to);
		return statement.executeQuery();
	}

	private void insertTag(int id, String tag) throws SQLException {
		PreparedStatement statement = statementProvider.get("insert-tag");
		statement.setInt(1, id);
		statement.setString(2, tag);
		statement.execute();
	}

	private void insertEntry(int tag, int feed, Instant value) throws SQLException {
		PreparedStatement statement = statementProvider.get("insert-entry");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		statement.setLong(3, value.toEpochMilli());
		statement.setString(4, labelOf(value));
		statement.execute();
	}

	private void insertEntry(int tag, int feed, double value) throws SQLException {
		PreparedStatement statement = statementProvider.get("insert-entry");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		statement.setDouble(3, value);
		statement.setNull(4, VARCHAR);
		statement.execute();
	}

	private void insertEntry(int tag, int feed, String value) throws SQLException {
		PreparedStatement statement = statementProvider.get("insert-entry");
		statement.setInt(1, tag);
		statement.setInt(2, feed);
		statement.setNull(3, BIGINT);
		statement.setString(4, value);
		statement.execute();
	}

	private void updateSize(int size) throws SQLException {
		PreparedStatement statement = statementProvider.get("update-feed");
		statement.setInt(1, size);
		statement.execute();
	}

	private void updateTag(int tag, int feed) throws SQLException {
		PreparedStatement statement = statementProvider.get("update-tag-feed");
		statement.setInt(1, feed);
		statement.setInt(2, tag);
		statement.execute();
	}

	private static String labelOf(Instant value) {
		if (value.equals(Legacy)) return "Legacy";
		if (value.equals(BigBang)) return "Big Bang";
		return value.toString().substring(0, 19).replace('T', ' ');
	}

	private Bundle bundleOf(ResultSet resultSet) throws SQLException {
		return new SqlBundle(resultSet);
	}

	static final String InitTables = """
					CREATE TABLE IF NOT EXISTS tags_[id] (
						tag INTEGER NOT NULL,
						label TEXT,
						feed INTEGER,
						PRIMARY KEY (tag)
					);
				
					CREATE TABLE IF NOT EXISTS map_[id] (
						feed INTEGER NOT NULL,
						tag INTEGER NOT NULL,
						num REAL,
						txt TEXT,
						PRIMARY KEY (feed, tag)
					);
					CREATE INDEX IF NOT EXISTS idx_tag_[id] ON map_[id](tag);
					CREATE INDEX IF NOT EXISTS idx_feed_[id] ON map_[id](feed);
					INSERT INTO map_[id] (tag, feed, num, txt) SELECT -1, -1, 0, NULL WHERE NOT EXISTS (SELECT 1 FROM map_[id]);
				""";

	private class StatementProvider {

		final Map<String, PreparedStatement> statements;

		StatementProvider() throws SQLException {
			this.statements = statements();
		}


		Map<String, PreparedStatement> statements() throws SQLException {
			Map<String, PreparedStatement> statements = new HashMap<>();
			statements.put("insert-tag", create("INSERT INTO tags_[id] (tag, label, feed) VALUES (?, ?, -1)"));
			statements.put("insert-entry", create("INSERT INTO map_[id] (tag, feed, num, txt) VALUES (?, ?, ?, ?)"));
			statements.put("update-tag-feed", create("UPDATE tags_[id] SET feed = ? WHERE tag = ?;"));
			statements.put("update-feed", create("UPDATE map_[id] SET num = ? WHERE tag = -1 AND feed = -1;"));
			statements.put("select-all", create("SELECT * FROM map_[id] ORDER BY feed, tag;"));
			statements.put("select-tags", create("SELECT tag, label, feed FROM tags_[id]"));
			statements.put("select-instants", create("SELECT feed, num FROM map_[id] WHERE tag = 0"));
			statements.put("select-double-value", create("SELECT num FROM map_[id] WHERE tag = ? AND feed = ?"));
			statements.put("select-double-values", create("SELECT feed, num FROM map_[id] WHERE tag = ? and feed BETWEEN ? AND ?"));
			statements.put("select-string-value", create("SELECT txt FROM map_[id] WHERE tag = ? AND feed = ?"));
			statements.put("select-string-values", create("SELECT feed, txt FROM map_[id] WHERE tag = ? and feed BETWEEN ? AND ?"));
			return statements;
		}

		PreparedStatement get(String sql) {
			return statements.get(sql);
		}

		private PreparedStatement create(String sql) throws SQLException {
			return connection.prepareStatement(sql.replace("[id]", id));
		}


	}



}
