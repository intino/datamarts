package io.intino.alexandria.datamarts.model.view.parser;

import io.intino.alexandria.datamarts.model.view.Format;

import java.io.InputStream;
import java.time.Period;

public interface FormatParser {
    Format parse(InputStream input) throws Exception;

     class PeriodParser {
        public static Period parse(String periodStr) {
            return getPeriod(periodStr);
        }

        static Period getPeriod(String periodStr) {
            String normalized = periodStr.trim().toUpperCase();
            if (normalized.matches("\\d+\\s+YEAR(S)?")) {
                int years = Integer.parseInt(normalized.split("\\s+")[0]);
                return Period.ofYears(years);
            } else if (normalized.matches("\\d+\\s+MONTH(S)?")) {
                int months = Integer.parseInt(normalized.split("\\s+")[0]);
                return Period.ofMonths(months);
            } else if (normalized.matches("\\d+\\s+DAY(S)?")) {
                int days = Integer.parseInt(normalized.split("\\s+")[0]);
                return Period.ofDays(days);
            } else {
                return Period.parse(periodStr);
            }
        }
    }
}
