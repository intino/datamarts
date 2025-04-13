package systems.intino.datamarts.subjectindex.io.feeders;

import systems.intino.datamarts.subjectindex.io.StatementFeeder;
import systems.intino.datamarts.subjectindex.model.Statement;

import java.io.*;
import java.util.Iterator;

public class RawStatementFeeder implements StatementFeeder {
	private final InputStream is;

	public RawStatementFeeder(InputStream is) {
		this.is = is;
	}

	@Override
	public Iterator<Statement> iterator() {
		return new BufferedReader(new InputStreamReader(is))
				.lines()
				.map(Statement::new)
				.onClose(this::close)
				.iterator();
	}

	private void close() {
		try {
			is.close();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}


}
