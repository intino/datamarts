package systems.intino.alexandria.datamarts.model.view.functions;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.time.ZoneOffset.UTC;

public interface TemporalFunction extends Function<Instant, Object> {
	Map<String, TemporalFunction> map = create();

	static Map<String, TemporalFunction> create() {
		Map<String, TemporalFunction> map = new HashMap<>();
		map.put("day-of-week", ts-> zdt(ts).getDayOfWeek().getValue());
		map.put("day-of-month", ts-> zdt(ts).getDayOfMonth());
		map.put("month-of-year", ts-> (zdt(ts).getMonthValue()));
		map.put("year", ts->  zdt(ts).getYear());
		map.put("quarter-of-year", ts-> (double) quarterOf(zdt(ts)));
		map.put("year-quarter", ts-> zdt(ts).getYear() + "Q" + quarterOf(zdt(ts)));
		map.put("year-month", ts-> zdt(ts).format(with("yyyyMM")));
		map.put("year-month-day", ts-> zdt(ts).format(with("yyyyMMdd")));
		map.put("year-month-day-hour", ts-> zdt(ts).format(with("yyyyMMddHH")));
		map.put("year-month-day-hour-minute", ts-> zdt(ts).format(with("yyyyMMddHHmm")));
		map.put("year-month-day-hour-minute-second", ts-> zdt(ts).format(with("yyyyMMddHHmmss")));
		return map;
	}


	static boolean isNumeric(String function) {
		return false;
	}


	static TemporalFunction of(String function) {
		return map.containsKey(function) ? map.get(function) : ts-> "Unknown function: " + function;
	}

	static boolean contains(String function) {
		return map.containsKey(function);
	}

	static ZonedDateTime zdt(Instant ts) {
		return ts.atZone(UTC);
	}

	private static Integer quarterOf(ZonedDateTime ts) {
		return (ts.getMonthValue() - 1) / 3 + 1;
	}

	private static DateTimeFormatter with(String pattern) {
		return DateTimeFormatter.ofPattern(pattern);
	}
}
