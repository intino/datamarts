package systems.intino.datamarts.subjectindex.io;

import systems.intino.datamarts.subjectindex.model.Statement;

import java.util.function.Function;

public interface StatementFeeder extends Iterable<Statement> {

	default Schema schema() {
		return (key, mapper) -> null;
	}

	interface Schema {
		Schema map(String key, Function<String,String> mapper);
	}

}
