package systems.intino.alexandria.datamarts.subjectstore;

import systems.intino.alexandria.datamarts.subjectstore.calculator.VectorCalculator;
import systems.intino.alexandria.datamarts.subjectstore.model.Filter;
import systems.intino.alexandria.datamarts.subjectstore.model.TemporalReferences;
import systems.intino.alexandria.datamarts.subjectstore.model.Vector;
import systems.intino.alexandria.datamarts.subjectstore.model.series.Sequence;
import systems.intino.alexandria.datamarts.subjectstore.model.series.Signal;
import systems.intino.alexandria.datamarts.subjectstore.model.vectors.DoubleVector;
import systems.intino.alexandria.datamarts.subjectstore.model.vectors.ObjectVector;
import systems.intino.alexandria.datamarts.subjectstore.model.view.Column;
import systems.intino.alexandria.datamarts.subjectstore.model.view.Format;
import systems.intino.alexandria.datamarts.subjectstore.model.view.fields.CategoricalField;
import systems.intino.alexandria.datamarts.subjectstore.model.view.fields.NumericalField;
import systems.intino.alexandria.datamarts.subjectstore.model.view.fields.TemporalField;
import systems.intino.alexandria.datamarts.subjectstore.model.view.format.YamlFileFormatReader;
import systems.intino.alexandria.datamarts.subjectstore.model.view.format.YamlFormatReader;

import java.io.*;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.*;

public class SubjectView {
	private final SubjectStore store;
	private final Format format;
	private final List<Instant> instants;
	private final Map<String, Vector<?>> vectors;

	public SubjectView(SubjectStore store, Format format) {
		this.store = store;
		this.format = format;
		this.instants = instants(format.from(), format.to(), format.duration());
		this.vectors = new HashMap<>();
		this.build();
	}

	public SubjectView(SubjectStore store, String format) {
		this(store, new YamlFormatReader(format).read());
	}

	public SubjectView(SubjectStore store, File format) throws IOException {
		this(store, new YamlFileFormatReader(format).read());
	}

	public List<Column> columns() {
		return format.columns();
	}

	private String column(int i) {
		return format.columns().get(i).name;
	}

	public Instant from() {
		return format.from();
	}

	public Instant to() {
		return format.to();
	}

	public TemporalAmount duration() {
		return format.duration();
	}

	public int rows() {
		return instants.size();
	}

	public void exportTo(File file) throws IOException {
		try (OutputStream os = new FileOutputStream(file)) {
			exportTo(os);
		}
	}

	public void exportTo(OutputStream os) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
			writer.write(tsv());
		}
	}

	private void build() {
		columns().forEach(this::build);
	}

	private void build(Column column) {
		vectors.put(column.name, calculate(column));
	}

	private Vector<?> calculate(Column column) {
		if (column.isAlphanumeric()) {
			return get(tagIn(column.definition), CategoricalField.of(fieldIn(column.definition)));
		}
		else {
			return filter(calculate(column.definition), column.filters);
		}
	}

	private DoubleVector calculate(String definition) {
		return vectorCalculator().calculate(definition);
	}

	private Vector<?> filter(Vector<?> input, List<Filter> filters) {
		return input instanceof DoubleVector vector ?
				filter(vector.values(), filters) :
				input;
	}

	private DoubleVector filter(double[] values, List<Filter> filters) {
		for (Filter filter : filters)
			values = filter.apply(values);
		return new DoubleVector(values);
	}

	private VectorCalculator vectorCalculator() {
		return new VectorCalculator(rows(), this::variable);
	}

	private DoubleVector variable(String name) {
		if (vectors.get(name) instanceof DoubleVector vector) return vector;
		try {
			String tag = tagIn(name);
			String field = fieldIn(name);
			if (isTemporal(tag) && TemporalField.contains(field)) return calculate(TemporalField.of(field));
			if (CategoricalField.contains(field)) return calculate(tag, CategoricalField.of(field));
			if (NumericalField.contains(field)) return calculate(tag, NumericalField.of(field));
		}
		catch (Exception ignored) {
		}
		throw new IllegalArgumentException("Variable not found: " + name);
	}

	private DoubleVector calculate(TemporalField temporalField) {
		double[] values = instants.stream().map(temporalField).mapToDouble(s -> (double) s).toArray();
		return new DoubleVector(values);
	}

	private DoubleVector calculate(String tag, NumericalField function) {
		Signal signal = store.numericalQuery(tag).get(from(), to());
		Signal[] segments = signal.segments(duration());
		double[] values = Arrays.stream(segments).map(function).mapToDouble(v -> v).toArray();
		return new DoubleVector(values);
	}

	private DoubleVector calculate(String tag, CategoricalField function) {
		Sequence sequence = store.categoricalQuery(tag).get(from(), to());
		Sequence[] segments = sequence.segments(duration());
		double[] values = Arrays.stream(segments).map(function).mapToDouble(s -> (double) s).toArray();
		return new DoubleVector(values);
	}

	private Vector<?> get(String attribute, CategoricalField function) {
		Sequence sequence = store.categoricalQuery(attribute).get(from(), to());
		Sequence[] segments = sequence.segments(duration());
		Object[] values = Arrays.stream(segments).map(function).toArray(Object[]::new);
		return new ObjectVector(values);
	}

	private boolean isTemporal(String tag) {
		return tag.equals("ts");
	}

	private String tagIn(String name) {
		return name.split("\\.")[0];
	}

	private String fieldIn(String name) {
		return name.split("\\.")[1];
	}

	private String tsv() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < rows(); i++) {
			StringJoiner row = new StringJoiner("\t");
			for (int j = 0; j < columns().size(); j++)
				row.add(String.valueOf(value(i, j)));
			sb.append(row).append('\n');
		}
		return sb.toString();
	}

	private Object value(int i, int j) {
		Object o = vectors.get(column(j)).get(i);
		return isEmpty(o) ? "" : o;
	}

	private boolean isEmpty(Object o) {
		if (o == null) return true;
		if (o instanceof Double d) return Double.isNaN(d);
		return false;
	}

	private static List<Instant> instants(Instant from, Instant instant, TemporalAmount duration) {
		return TemporalReferences
				.iterate(from, instant, duration)
				.toList();
	}

	@Override
	public String toString() {
		return "Table(" + format + ")";
	}



}
