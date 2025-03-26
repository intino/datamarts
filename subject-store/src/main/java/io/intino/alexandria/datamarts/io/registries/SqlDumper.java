package io.intino.alexandria.datamarts.io.registries;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

class SqlDumper {
	private final String header;
	private final List<String> tags;
	private final ResultSet rs;

	SqlDumper(ResultSet rs, List<String> tags, String type) throws SQLException {
		this.rs = rs;
		this.tags = tags;
		this.header = '[' + type + ']' + '\n';
		this.rs.next();
	}

	void execute(OutputStream os) throws SQLException, IOException {
		int last = -1;
		while (rs.next()) {
			int feed = rs.getInt(1);
			if (feed != last) os.write(header.getBytes());
			os.write(entry().getBytes());
			last = feed;
		}

	}

	String entry() throws SQLException {
		return entry(rs.getInt(2), rs.getDouble(3), rs.getString(4)) + "\n";
	}

	String entry(int tag, double value, String text) {
		if (tag == 0) return "ts=" + Instant.ofEpochMilli((long) value).toString();
		if (tag == 1) return "ss=" + text;
		return tags.get(tag - 2) + '=' + (text != null ? text : value);
	}


}
