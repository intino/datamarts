package systems.intino.alexandria.datamarts;

import systems.intino.alexandria.datamarts.io.Feed;
import systems.intino.alexandria.datamarts.io.Registry;
import systems.intino.alexandria.datamarts.model.TemporalReferences;
import systems.intino.alexandria.datamarts.model.Point;
import systems.intino.alexandria.datamarts.io.registries.SqliteRegistry;
import systems.intino.alexandria.datamarts.model.series.Sequence;
import systems.intino.alexandria.datamarts.model.series.Signal;

import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static systems.intino.alexandria.datamarts.model.TemporalReferences.BigBang;
import static systems.intino.alexandria.datamarts.model.TemporalReferences.Legacy;

public class SubjectStore implements Closeable {
	private final Registry registry;

	public SubjectStore(File file) {
		this.registry = new SqliteRegistry(file);
	}

	public SubjectStore(String name) {
		this.registry = new SqliteRegistry(name);
	}

	public String name() {
		return registry.name();
	}

	public String type() {
		return registry.type();
	}

	public String id() {
		return registry.id();
	}

	public int 	size() {
		return registry.size();
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

	public void dump(File file) throws IOException {
		try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
			dump(os);
		}
	}

	public void dump(OutputStream os) {
		registry.dump(os);
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


	public Batch batch() {
		return new Batch() {
			private final List<Feed> feeds = new ArrayList<>();
			@Override
			public Transaction feed(Instant instant, String source) {
				return new Transaction() {
					private final Feed feed = new Feed(instant, source);
					@Override
					public Transaction add(String tag, double value) {
						feed.put(tag, value);
						return this;

					}

					@Override
					public Transaction add(String tag, String value) {
						feed.put(tag, value);
						return this;
					}

					@Override
					public void terminate() {
						if (feed.isEmpty()) return;
						feeds.add(feed);
					}
				};
			}

			@Override
			public void terminate() {
				registry.register(feeds);
			}
		};
	}

	public Transaction feed(Instant instant, String source) {
		return transaction(new Feed(instant, source));
	}

	private Transaction transaction(Feed feed) {
		return new Transaction() {
			@Override
			public Transaction add(String tag, String value) {
				feed.put(tag, value);
				return this;
			}

			@Override
			public Transaction add(String tag, double value) {
				feed.put(tag, value);
				return this;
			}


			@Override
			public void terminate() {
				if (feed.isEmpty()) return;
				registry.register(List.of(feed));
			}
		};
	}

	public interface Batch {
		Transaction feed(Instant instant, String source);
		void terminate();
	}

	public interface Transaction {
		Transaction add(String tag, double value);
		Transaction add(String tag, String value);
		void terminate();
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
