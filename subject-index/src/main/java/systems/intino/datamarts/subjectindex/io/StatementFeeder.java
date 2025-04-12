package systems.intino.datamarts.subjectindex.io;

import systems.intino.datamarts.subjectindex.io.feeders.TabularDataFeeder;
import systems.intino.datamarts.subjectindex.model.Statement;

import java.io.*;
import java.util.function.Function;

public interface StatementFeeder extends Iterable<Statement> {

	Schema schema();
	
	static StatementFeeder fromTabularData(InputStream is) {
		return new TabularDataFeeder(is);
	}

	interface Schema {
		Schema map(String key, Function<String,String> mapper);
	}

}
