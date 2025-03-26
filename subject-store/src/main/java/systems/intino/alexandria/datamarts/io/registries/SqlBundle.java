package systems.intino.alexandria.datamarts.io.registries;

import systems.intino.alexandria.datamarts.model.Bundle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Iterator;

public class SqlBundle implements Bundle {
	private final Cursor cursor;

	public SqlBundle(ResultSet resultSet) {
		this.cursor = new Cursor(resultSet);
	}

	@Override
	public void close() {
		cursor.close();
	}

	@Override
	public Iterator<Tuple> iterator() {
		return new Iterator<>() {

			@Override
			public boolean hasNext() {
				return cursor.next();
			}

			@Override
			public Tuple next() {
				return item();
			}
		};
	}

	private Tuple item() {
		return index -> new Tuple.Data() {
			@Override
			public int asInt() {
				return cursor.getInt(index);
			}

			@Override
			public long asLong() {
				return cursor.getLong(index);
			}

			@Override
			public double asDouble() {
				return cursor.getDouble(index);
			}

			@Override
			public Instant asInstant() {
				return Instant.ofEpochMilli(cursor.getLong(index));
			}

			@Override
			public String asString() {
				return cursor.getString(index);
			}
		};
	}

	private record Cursor(ResultSet rs) {

		public int getInt(int index) {
			try {
				return rs.getInt(index);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		public long getLong(int index) {
			try {
				return rs.getLong(index);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		public double getDouble(int index) {
			try {
				return rs.getDouble(index);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		public String getString(int index) {
			try {
				return rs.getString(index);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		public void close() {
			try {
				rs.close();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		public boolean next() {
			try {
				return rs.next();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

	}
}





