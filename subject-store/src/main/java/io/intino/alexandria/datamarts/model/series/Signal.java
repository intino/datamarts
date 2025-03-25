package io.intino.alexandria.datamarts.model.series;

import io.intino.alexandria.datamarts.model.TemporalReferences;
import io.intino.alexandria.datamarts.model.Point;
import io.intino.alexandria.datamarts.model.Series;
import io.intino.alexandria.datamarts.model.series.signal.Distribution;
import io.intino.alexandria.datamarts.model.series.signal.Summary;

import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.List;

public interface Signal extends Series<Double> {
	default double[] values() { return stream().mapToDouble(Point::value).toArray(); }
	default Signal[] segments(TemporalAmount duration) { return splitBy(from(), to(), duration); }
	default Signal[] segments(int number) { return segments(duration().dividedBy(number)); }
	default Summary summary() { return Summary.of(this); }
	default Distribution distribution() { return Distribution.of(this); }

	private Segment[] splitBy(Instant from, Instant to, TemporalAmount duration) {
		return  TemporalReferences.iterate(from, to, duration)
				.map(current -> new Segment(current, TemporalReferences.add(current, duration), this))
				.toArray(Segment[]::new);
	}


	final class Raw extends Series.Raw<Double> implements Signal {

		public Raw(Instant from, Instant to, List<Point<Double>> points) {
			super(from, to, points);
		}

	}

	final class Segment extends Series.Segment<Double> implements Signal {
		public Segment(Instant from, Instant to, Signal parent) {
			super(from, to, parent);
		}

	}
}
