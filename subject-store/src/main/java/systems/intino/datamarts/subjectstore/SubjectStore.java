package systems.intino.datamarts.subjectstore;

import systems.intino.datamarts.subjectstore.io.Feed;
import systems.intino.datamarts.subjectstore.io.Registry;
import systems.intino.datamarts.subjectstore.io.registries.SqliteConnection;
import systems.intino.datamarts.subjectstore.io.Bundle;
import systems.intino.datamarts.subjectstore.model.TemporalReferences;
import systems.intino.datamarts.subjectstore.model.Point;
import systems.intino.datamarts.subjectstore.io.registries.SqlRegistry;
import systems.intino.datamarts.subjectstore.model.series.Sequence;
import systems.intino.datamarts.subjectstore.model.series.Signal;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

import static java.util.Comparator.comparingInt;
import static systems.intino.datamarts.subjectstore.model.TemporalReferences.BigBang;
import static systems.intino.datamarts.subjectstore.model.TemporalReferences.Legacy;

public class SubjectStore implements Closeable {
	private final String subject;
	private final Registry registry;
	private final TagSet tagSet;
	private final Timeline timeline;
	private final Connection connection;
	private final boolean closeable;

	public SubjectStore(String subject, File file) {
		this.subject = subject;
		this.connection = SqliteConnection.from(file);
		this.registry = new SqlRegistry(subject, connection);
		this.tagSet = new TagSet(registry.tags());
		this.timeline = new Timeline(registry.instants());
		this.closeable = true;
	}

	public SubjectStore(String subject) {
		this.subject = subject;
		this.connection = SqliteConnection.inMemory();
		this.registry = new SqlRegistry(subject, connection);
		this.tagSet = new TagSet(registry.tags());
		this.timeline = new Timeline(registry.instants());
		this.closeable = true;
	}

	public SubjectStore(String subject, Connection connection) {
		this.subject = subject;
		this.connection = connection;
		this.registry = new SqlRegistry(subject, connection);
		this.tagSet = new TagSet(registry.tags());
		this.timeline = new Timeline(registry.instants());
		this.closeable = false;
	}

	public String identifier() {
		int i = subject.lastIndexOf('/');
		return subject.substring(i + 1).trim();
	}

	public String name() {
		String identifier = identifier();
		int i = identifier.lastIndexOf('.');
		return i >= 0 ? identifier.substring(0, i) : identifier;
	}

	public String type() {
		String identifier = identifier();
		int i = identifier.lastIndexOf('.');
		return i >= 0 ? identifier.substring(i + 1) : "";
	}

	public int size() {
		return registry.size();
	}

	public Instant first() {
		return timeline.isEmpty() ? null : timeline.getFirst();
	}

	public Instant last() {
		return registry.isEmpty() ? null : timeline.getLast();
	}

	public boolean legacyExists() {
		return timeline.contains(Legacy);
	}

	public boolean bigbangExists() {
		return timeline.contains(BigBang);
	}

	public boolean legacyPending() {
		return legacyExists() && !bigbangExists();
	}

	public List<Instant> instants() {
		return timeline.instants();
	}

	public List<String> tags() {
		return tagSet.tags();
	}

	public boolean exists(String tag) {
		return tags().contains(tag);
	}

	public String ss(int feed) {
		return registry.ss(feed);
	}

	public Double currentNumber(String tag) {
		Point<Double> current = numericalQuery(tag).get();
		return current != null ? current.value() : null;
	}

	public String currentText(String tag) {
		Point<String> current = categoricalQuery(tag).get();
		return current != null ? current.value() : null;
	}

	public NumericalQuery numericalQuery(String tag) {
		return new NumericalQuery(tag);
	}

	public CategoricalQuery categoricalQuery(String tag) {
		return new CategoricalQuery(tag);
	}

	private Point<Double> readNumber(String tag) {
		int feed = tagSet.lastUpdatingFeedOf(tag);
		if (feed == -1) return null;
		return new Point<>(
				feed,
				timeline.get(feed),
				registry.getNumber(tagSet.get(tag), feed)
		);
	}

	private List<Point<Double>> readNumbers(String tag, Instant from, Instant to) {
		return readNumbers(registry.getNumbers(tagSet.get(tag), timeline.from(from), timeline.to(to)));
	}

	private List<Point<Double>> readNumbers(Bundle bundle) {
		try {
			List<Point<Double>> result = new ArrayList<>();
			for (Bundle.Tuple tuple : bundle) {
				int feed = tuple.at(1).asInt();
				result.add(new Point<>(feed, timeline.get(feed), tuple.at(2).asDouble()));
			}
			return result;
		}
		finally {
			bundle.close();
		}
	}

	private Point<String> readText(String tag) {
		int feed = tagSet.lastUpdatingFeedOf(tag);
		if (feed == -1) return null;
		return new Point<>(
				feed,
				timeline.get(feed),
				registry.getText(tagSet.get(tag), feed)
		);
	}

	private List<Point<String>> readTexts(String tag, Instant from, Instant to) {
		return readTexts(registry.getTexts(tagSet.get(tag), timeline.from(from), timeline.to(to)));
	}

	private List<Point<String>> readTexts(Bundle bundle) {
		try {
			List<Point<String>> result = new ArrayList<>();
			for (Bundle.Tuple tuple : bundle) {
				int feed = tuple.at(1).asInt();
				result.add(new Point<>(feed, timeline.get(feed), tuple.at(2).asString()));
			}
			return result;
		}
		finally {
			bundle.close();
		}
	}

	public void dump(File file) throws IOException {
		try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
			dump(os);
		}
	}

	public void dump(OutputStream os) {
		registry.dump(os, dictionary());
	}

	public void restore(File file) throws IOException {
		try (InputStream is = new FileInputStream(file)) {
			restore(is);
		}
	}

	public void restore(InputStream is) throws IOException {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
			new Restorer(reader).execute();
		}
	}

	private Map<Object, String> dictionary() {
		Map<Object, String> result = new HashMap<>();
		result.put("id", name());
		result.put("type", type());
		for (String tag : tagSet.tags())
			result.put(tagSet.get(tag), tag);
		return result;
	}

	public class NumericalQuery {
		private final String tag;

		public NumericalQuery(String tag) {
			this.tag = tag;
		}

		public Point<Double> get() {
			return readNumber(tag);
		}

		public Signal all() {
			return get(first(), last());
		}

		public Signal get(Instant from, Instant to) {
			return new Signal.Raw(from, to, readNumbers(tag, from, to));
		}

		public Signal get(TemporalReferences.TimeSpan span) {
			return get(span.from(), span.to());
		}

		@Override
		public String toString() {
			return "NumericalQuery(" + tag + ')';
		}
	}

	public class CategoricalQuery {

		private final String tag;

		public CategoricalQuery(String tag) {
			this.tag = tag;
		}

		public Point<String> get() {
			return readText(tag);
		}

		public Sequence all() {
			return get(first(), last());
		}

		public Sequence get(Instant from, Instant to) {
			return new Sequence.Raw(from, to, readTexts(tag, from, to));
		}

		public Sequence get(TemporalReferences.TimeSpan span) {
			return get(span.from(), span.to());
		}

		@Override
		public String toString() {
			return "CategoricalQuery(" + tag + ')';
		}
	}
	
	public Transaction on(Instant instant, String source) {
		return transaction(new Feed(instant, source));
	}

	private Transaction transaction(Feed feed) {
		return new Transaction() {
			private boolean ignored = false;

			@Override
			public Transaction put(String tag, String value) {
				feed.put(tag, value);
				return this;
			}

			@Override
			public Transaction ignore(boolean value) {
				this.ignored = value;
				return this;
			}

			@Override
			public Transaction put(String tag, double value) {
				feed.put(tag, value);
				return this;
			}

			@Override
			public void commit() {
				if (feed.isEmpty() || ignored) return;
				SubjectStore.this.put(feed);
				registry.push();
			}
		};
	}

	public Batch batch() {
		return new Batch() {
			private final List<Feed> feeds = new ArrayList<>();
			@Override
			public Transaction on(Instant instant, String source) {
				return new Transaction() {
					private boolean ignored = false;
					private final Feed feed = new Feed(instant, source);
					@Override
					public Transaction put(String tag, double value) {
						feed.put(tag, value);
						return this;
					}

					@Override
					public Transaction put(String tag, String value) {
						feed.put(tag, value);
						return this;
					}

					@Override
					public Transaction ignore(boolean value) {
						this.ignored = value;
						return this;
					}

					@Override
					public void commit() {
						if (feed.isEmpty() || ignored) return;
						feeds.add(feed);
					}
				};
			}

			@Override
			public void terminate() {
				feeds.forEach(feed -> put(feed));
				registry.push();
			}
		};
	}

	private void put(Feed feed) {
		int id = registry.nextFeed();
		insertInstant(id, feed.instant);
		insertTags(feed.tags());
		insertFacts(id, feed);
	}

	private void insertInstant(int id, Instant instant) {
		timeline.add(instant, id);
	}

	private void insertTags(Set<String> tags) {
		for (String tag : tags) {
			int id = tagSet.add(tag);
			if (id < 0) continue;
			registry.setTag(id, tag);
		}
	}

	private void insertFacts(int id, Feed feed) {
		put("ts", feed.instant);
		put("ss", feed.source);
		for (String tag : feed.tags()) {
			put(tag, feed.get(tag));
			updateTag(tag, id, feed.instant);
		}
	}

	private void updateTag(String tag, int id, Instant instant) {
		if (instant.equals(Legacy)) return;
		if (lastUpdatingInstantOf(tag).isAfter(instant)) return;
		tagSet.update(tag, id);
		registry.setTagLastFeed(tagSet.get(tag), id);
	}

	private Instant lastUpdatingInstantOf(String tag) {
		return timeline.get(tagSet.lastUpdatingFeedOf(tag));
	}

	private void put(String tag, Object value) {
		registry.put(tagSet.get(tag), value);
	}

	public interface Transaction {
		Transaction put(String tag, double value);
		Transaction put(String tag, String value);
		Transaction ignore(boolean value);
		void commit();
	}

	public interface Batch {
		Transaction on(Instant instant, String source);
		void terminate();
	}

	@Override
	public String toString() {
		return subject;
	}

	@Override
	public void close() {
		try {
			if (closeable)
				connection.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static class RegistryException extends RuntimeException {
		public RegistryException(Exception exception) {
			super(exception);
		}
	}

	public static class TagSet {
		private final Map<String, Integer> labels;
		private final Map<Integer, Integer> lastUpdatingFeeds;

		TagSet(Bundle bundle)  {
			this.labels = new HashMap<>();
			this.lastUpdatingFeeds = new HashMap<>();
			this.init(bundle);
		}

		int lastUpdatingFeedOf(String tag) {
			return contains(tag) ? lastUpdatingFeedOf(get(tag)) : -1;
		}

		int lastUpdatingFeedOf(int tag) {
			return lastUpdatingFeeds.getOrDefault(tag, -1);
		}

		int get(String tag) {
			return labels.get(tag);
		}

		List<String> tags() {
			return labels.entrySet().stream()
					.filter(e->e.getValue() > 1)
					.sorted(comparingInt(Map.Entry::getValue))
					.map(Map.Entry::getKey)
					.toList();
		}
		
		private void init(Bundle bundle) {
			for (Bundle.Tuple tuple : bundle)
				init(tuple.at(1).asInt(), tuple.at(2).asString(), tuple.at(3).asInt());
			bundle.close();
		}

		private void init(int id, String label, int feed) {
			labels.put(label, id);
			lastUpdatingFeeds.put(id, feed);
		}

		private int add(String tag)  {
			if (contains(tag)) return -1;
			int index = labels.size();
			labels.put(tag, index);
			return index;
		}

		private void update(String tag, int feed) {
			lastUpdatingFeeds.put(get(tag), feed);
		}

		public boolean contains(String tag) {
			return labels.containsKey(tag);
		}

	}

	public static class Timeline {
		private final Map<Integer, Instant> instants;

		Timeline(Bundle bundle) {
			this.instants = new HashMap<>();
			this.init(bundle);
		}

		Instant get(int feed) {
			if (feed < 0) return java.time.Instant.MIN;
			return instants.get(feed);
		}

		List<Instant> instants() {
			return new ArrayList<>(instants.values()).stream()
					.sorted()
					.toList();
		}

		public boolean contains(Instant instant) {
			return instants.values().stream().anyMatch(v->v.equals(instant));
		}

		public Instant getFirst() {
			return instants().getFirst();
		}

		public Instant getLast() {
			return instants().getLast();
		}

		public boolean isEmpty() {
			return instants.isEmpty();
		}

		void add(Instant instant, int feed) {
			instants.put(feed, instant);
		}

		private void init(Bundle bundle) {
			for (Bundle.Tuple tuple : bundle)
				instants.put(tuple.at(1).asInt(), tuple.at(2).asInstant());
			bundle.close();
		}

		private int from(Instant from) {
			if (from.equals(java.time.Instant.MIN)) return 0;
			int index = instants.size();
			Instant min = java.time.Instant.MAX;
			for (Map.Entry<Integer, Instant> entry : instants.entrySet()) {
				Instant instant = entry.getValue();
				if (instant.isBefore(from)) continue;
				if (instant.isAfter(min)) continue;
				index = entry.getKey();
				min = instant;
			}
			return index;
		}

		private int to(Instant to) {
			if (to.equals(java.time.Instant.MAX)) return instants.size();
			int index = -1;
			Instant max = java.time.Instant.MIN;
			for (Map.Entry<Integer, Instant> entry : instants.entrySet()) {
				Instant instant = entry.getValue();
				if (instant.isAfter(to)) continue;
				if (instant.isBefore(max)) continue;
				index = entry.getKey();
				max = instant;
			}
			return index;
		}
	}

	private final class Restorer {
		private final BufferedReader reader;
		private final Batch batch;
		private Transaction transaction;

		private Restorer(BufferedReader reader) {
			this.reader = reader;
			this.batch = batch();
			this.transaction = batch.on(Instant.now(), "restore");
		}

		public void execute() throws IOException {
				while (true) {
					if (process(reader.readLine())) continue;
					break;
				}
				transaction.commit();
				batch.terminate();
		}

		private boolean process(String line) throws IOException {
			if (line == null) return false;
			return line.startsWith("[") ?
					startNewTransaction(line) :
					put(line);
		}

		private boolean startNewTransaction(String line) throws IOException {
			transaction.commit();
			transaction = batch.on(readInstant(), read("ss"))
					.ignore(!line.equals("[" + type() + "]"));
			return true;
		}

		private boolean put(String line) {
			return put(line.split("="));
		}

		private boolean put(String[] data) {
			assert data.length == 2;
			if (data[0].equals("id"))
				transaction.ignore(!data[1].equals(name()));
			else
				put(data[0], data[1]);
			return true;
		}

		private void put(String tag, String value) {
			try {
				transaction.put(tag, Double.parseDouble(value));
			} catch (NumberFormatException e) {
				transaction.put(tag, value);
			}
		}

		private Instant readInstant() throws IOException {
			return Instant.parse(read("ts"));
		}

		private String read(String line) throws IOException {
			String[] data = reader.readLine().split("=");
			assert data.length == 2;
			assert data[0].equals(line);
			return data[1];
		}
	}
	
}
