package systems.intino.alexandria.datamarts.model.series;

import systems.intino.alexandria.datamarts.model.TemporalReferences;
import systems.intino.alexandria.datamarts.model.Point;
import systems.intino.alexandria.datamarts.model.Series;
import systems.intino.alexandria.datamarts.model.series.sequence.Summary;

import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.List;

import static systems.intino.alexandria.datamarts.model.TemporalReferences.iterate;

public interface Sequence extends Series<String> {
	default String[] values() { return stream().map(Point::value).toArray(String[]::new); }
	default String[] distinct() { return stream().map(Point::value).distinct().toArray(String[]::new); }
	default Sequence[] segments(TemporalAmount duration) { return splitBy(from(), to(), duration); }
	default Sequence[] segments(int number) { return segments(duration().dividedBy(number)); }
	default Summary summary() { return Summary.of(this); }

	private Segment[] splitBy(Instant from, Instant to, TemporalAmount duration) {
		return iterate(from, to, duration)
				.map(current -> new Segment(current, TemporalReferences.add(current, duration), this))
				.toArray(Segment[]::new);
	}

	final class Raw extends Series.Raw<String> implements Sequence {
		public Raw(Instant from, Instant to, List<Point<String>> points) {
			super(from, to, points);
		}
	}

	final class Segment extends Series.Segment<String> implements Sequence {

		public Segment(Instant from, Instant to, Sequence parent) {
			super(from, to, parent);
		}

	}
}
