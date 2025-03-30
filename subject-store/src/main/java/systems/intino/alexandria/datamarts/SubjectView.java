package systems.intino.alexandria.datamarts;

import systems.intino.alexandria.datamarts.calculator.VectorCalculator;
import systems.intino.alexandria.datamarts.io.Feed;
import systems.intino.alexandria.datamarts.model.Filter;
import systems.intino.alexandria.datamarts.model.TemporalReferences;
import systems.intino.alexandria.datamarts.model.vectors.DoubleVector;
import systems.intino.alexandria.datamarts.model.vectors.ObjectVector;
import systems.intino.alexandria.datamarts.model.view.Format;
import systems.intino.alexandria.datamarts.model.view.Column;
import systems.intino.alexandria.datamarts.model.series.Sequence;
import systems.intino.alexandria.datamarts.model.series.Signal;
import systems.intino.alexandria.datamarts.model.Vector;
import systems.intino.alexandria.datamarts.model.view.functions.CategoricalFunction;
import systems.intino.alexandria.datamarts.model.view.functions.NumericalFunction;
import systems.intino.alexandria.datamarts.model.view.functions.TemporalFunction;

import java.io.*;
import java.time.*;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.stream.Stream;

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
		vectors.put(column.name, apply(calculate(column), column.filters));
	}

	private static Vector<?> apply(Vector<?> input, List<Filter> filters) {
		return input instanceof DoubleVector vector ?
				apply(vector.values(), filters) :
				input;
	}

	private static DoubleVector apply(double[] values, List<Filter> filters) {
		for (Filter filter : filters)
			values = filter.apply(values);
		return new DoubleVector(values);
	}

	private Vector<?> calculate(Column column) {
		return switch (column.type) {
			case Temporal -> calculate(TemporalFunction.of(column.function));
			case Numerical -> calculate(NumericalFunction.of(column.function), column.attribute);
			case Categorical -> calculate(CategoricalFunction.of(column.function), column.attribute, CategoricalFunction.isNumeric(column.function));
			case Formula -> calculate(column.function);
		};
	}

	private Vector<?> calculate(TemporalFunction temporalFunction) {
		Stream<Object> values = instants.stream().map(temporalFunction);
		return toObjectVector(values);
	}

	private Vector<?> calculate(NumericalFunction function, String attribute) {
		Signal signal = store.numericalQuery(attribute).get(from(), to());
		Signal[] segments = signal.segments(duration());
		double[] values = Arrays.stream(segments).map(function).mapToDouble(v -> v).toArray();
		return new DoubleVector(values);
	}

	private Vector<?> calculate(CategoricalFunction function, String attribute, boolean isNumeric) {
		Sequence sequence = store.categoricalQuery(attribute).get(from(), to());
		Sequence[] segments = sequence.segments(duration());
		Stream<Object> values = Arrays.stream(segments).map(function);
		return isNumeric ?
				toDoubleVector(values) :
				toObjectVector(values);
	}

	private Vector<?> toDoubleVector(Stream<Object> values) {
		return new DoubleVector(values.mapToDouble(v-> (double) v).toArray());
	}

	private Vector<?> toObjectVector(Stream<Object> values) {
		return new ObjectVector(values.toArray(Object[]::new));
	}

	private Vector<?> calculate(String formula) {
		VectorCalculator calculator = new VectorCalculator(rows());
		for (String name : vectors.keySet()) {
			Vector<?> vector = vectors.get(name);
			if (vector instanceof DoubleVector v) calculator.add(name, v);
		}
		return calculator.calculate(formula);
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
