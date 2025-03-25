package io.intino.alexandria.datamarts;

import io.intino.alexandria.datamarts.io.Registry;
import io.intino.alexandria.datamarts.io.Transaction;
import io.intino.alexandria.datamarts.model.TemporalReferences;
import io.intino.alexandria.datamarts.model.Point;
import io.intino.alexandria.datamarts.io.registries.SqliteRegistry;
import io.intino.alexandria.datamarts.model.series.Sequence;
import io.intino.alexandria.datamarts.model.series.Signal;

import java.io.*;
import java.time.Instant;
import java.util.List;

import static io.intino.alexandria.datamarts.model.TemporalReferences.BigBang;
import static io.intino.alexandria.datamarts.model.TemporalReferences.Legacy;

public class SubjectStore implements Closeable {
	private final Registry registry;

	public SubjectStore(File file) {
		this.registry = new SqliteRegistry(file);
	}

	public String name() {
		return registry.name();
	}

	public int feeds() {
		return registry.feeds();
	}

	public Instant first() {
		return registry.isEmpty() ? null : registry.instants().getFirst();
	}

	public Instant last() {
		return registry.isEmpty() ? null : registry.instants().getLast();
	}

	public boolean legacyExists() {
		return registry.instants().contains(Legacy);
	}

	public boolean bigbangExists() {
		return registry.instants().contains(BigBang);
	}

	public boolean legacyPending() {
		return legacyExists() && !bigbangExists();
	}

	public List<Instant> instants() {
		return registry.instants();
	}

	public List<String> tags() {
		return registry.tags();
	}

	public boolean exists(String tag) {
		return tags().contains(tag);
	}

	public String ss(int feed) {
		return registry.ss(feed);
	}

	public NumericalQuery numericalQuery(String tag) {
		return new NumericalQuery(tag);
	}

	public CategoricalQuery categoricalQuery(String tag) {
		return new CategoricalQuery(tag);
	}

	public void export(File file) throws IOException {
		try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
			export(os);
		}
	}

	public void export(OutputStream os) throws IOException {
		try (Writer writer = new OutputStreamWriter(os)) {
			//TODO
			os.write("//TODO\n".getBytes());
		}
	}

	public class CategoricalQuery {
		private final String tag;

		public CategoricalQuery(String tag) {
			this.tag = tag;
		}

		public Point<String> current() {
			return registry.readText(tag);
		}

		public Sequence sequence(Instant from, Instant to) {
			return new Sequence.Raw(from, to, registry.readTexts(tag, from, to));
		}

		public Sequence sequence(TemporalReferences.TimeSpan span) {
			return sequence(span.from(), span.to());
		}

		@Override
		public String toString() {
			return "TextQuery(" + tag + ')';
		}
	}

	public class NumericalQuery {
		private final String tag;

		public NumericalQuery(String tag) {
			this.tag = tag;
		}

		public Point<Double> current() {
			return registry.readNumber(tag);
		}

		public Signal signal(Instant from, Instant to) {
			return new Signal.Raw(from, to, registry.readNumbers(tag, from, to));
		}

		public Signal signal(TemporalReferences.TimeSpan span) {
			return signal(span.from(), span.to());
		}

		@Override
		public String toString() {
			return "LongQuery(" + tag + ')';
		}
	}

	public Feed feed(Instant instant, String source) {
		return feed(new Transaction(instant, source));
	}

	private Feed feed(Transaction transaction) {
		return new Feed() {
			@Override
			public Feed add(String tag, String value) {
				transaction.put(tag, value);
				return this;
			}

			@Override
			public Feed add(String tag, double value) {
				transaction.put(tag, value);
				return this;
			}

			@Override
			public void execute() {
				registry.register(transaction);
			}
		};
	}

	public interface Feed {
		Feed add(String name, double value);
		Feed add(String name, String value);
		void execute();
	}

	@Override
	public String toString() {
		return registry.name();
	}

	@Override
	public void close() throws IOException {
		registry.close();
	}

	public static class RegistryException extends RuntimeException {
		public RegistryException(Exception exception) {
			super(exception);
		}
	}
}
