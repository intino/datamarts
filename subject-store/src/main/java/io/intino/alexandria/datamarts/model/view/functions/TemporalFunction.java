package io.intino.alexandria.datamarts.model.view.functions;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.time.ZoneOffset.UTC;

public interface TemporalFunction extends Function<Instant, Object> {

	TemporalFunction DayOfWeek = ts-> zdt(ts).getDayOfWeek().getValue();
	TemporalFunction DayOfMonth = ts-> zdt(ts).getDayOfMonth();
	TemporalFunction MonthOfYear = ts-> zdt(ts).getMonthValue();
	TemporalFunction QuarterOfYear = ts-> quarterOf(zdt(ts));
	TemporalFunction Year = ts-> zdt(ts).getYear();
	TemporalFunction YearQuarter = ts-> zdt(ts).getYear() + "Q" + quarterOf(zdt(ts));
	TemporalFunction YearMonth = ts-> zdt(ts).format(with("yyyyMM"));
	TemporalFunction YearMonthDay = ts-> zdt(ts).format(with("yyyyMMdd"));
	TemporalFunction YearMonthDayHour = ts-> zdt(ts).format(with("yyyyMMddHH"));
	TemporalFunction YearMonthDayHourMinute = ts-> zdt(ts).format(with("yyyyMMddHHmm"));
	TemporalFunction YearMonthDayHourMinuteSecond = ts-> zdt(ts).format(with("yyyyMMddHHmmss"));

	static ZonedDateTime zdt(Instant ts) {
		return ts.atZone(UTC);
	}

	private static Integer quarterOf(ZonedDateTime ts) {
		return (ts.getMonthValue() - 1) / 3 + 1;
	}

	private static DateTimeFormatter with(String pattern) {
		return DateTimeFormatter.ofPattern(pattern);
	}

	Map<String, TemporalFunction> FUNCTIONS_MAP = new HashMap<>() {{
		put("DAYOFWEEK", DayOfWeek);
		put("DAYOFMONTH", DayOfMonth);
		put("MONTHOFYEAR", MonthOfYear);
		put("QUARTEROFYEAR", QuarterOfYear);
		put("YEAR", Year);
		put("YEARQUARTER", YearQuarter);
		put("YEARMONTH", YearMonth);
		put("YEARMONTHDAY", YearMonthDay);
		put("YEARMONTHDAYHOUR", YearMonthDayHour);
		put("YEARMONTHDAYHOURMINUTE", YearMonthDayHourMinute);
		put("YEARMONTHDAYHOURMINUTESECOND", YearMonthDayHourMinuteSecond);
	}};

	 static TemporalFunction fromString(String functionName) {
		TemporalFunction function = FUNCTIONS_MAP.get(functionName.toUpperCase());
		if (function == null) {
			throw new IllegalArgumentException("Unknown Function: " + functionName);
		}
		return function;
	}

}
