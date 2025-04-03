package systems.intino.alexandria.datamarts.subjectstore.io.registries;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;

class SqlDumper {
	private final String header;
	private final String id;
	private final Map<Object, String> dictionary;
	private final ResultSet rs;

	SqlDumper(ResultSet rs, Map<Object, String> dictionary) throws SQLException {
		this.rs = rs;
		this.dictionary = dictionary;
		this.header = '[' + type() + ']' + '\n';
		this.id = id();
		this.rs.next();
	}

	private String id() {
		return dictionary.get("id");
	}

	private String type() {
		return dictionary.get("type");
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
		if (tag == 1) return "ss=" + text + "\n" + "id=" + id;
		return dictionary.get(tag) + '=' + (text != null ? text : value);
	}


}
