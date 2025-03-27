package tests;

import io.intino.alexandria.datamarts.SubjectStore;
import io.intino.alexandria.datamarts.SubjectView;
import io.intino.alexandria.datamarts.model.view.Column;
import io.intino.alexandria.datamarts.model.view.Format;
import io.intino.alexandria.datamarts.model.view.functions.TemporalFunction;
import io.intino.alexandria.datamarts.model.view.parser.FormatParser;
import io.intino.alexandria.datamarts.model.view.parser.YamlFormatParser;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.stream.Stream;

import static java.time.temporal.ChronoUnit.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

@SuppressWarnings("NewClassNamingConvention")
public class FormatParser_ {
    private final static Instant from = Instant.parse("2025-01-01T00:00:00Z");
    private final static Instant to = Instant.parse("2025-02-01T00:00:00Z");
    private final static String expected = """
        1	1	4.0	24.0	20.0	28.0	cloudy	20250101
        1	2	3.0	18.0	18.0	18.0	rain	20250201
        """;

    @Test
    public void shouldParseValidYamlConfiguration() throws Exception {
        String yamlContent = """
        From: "2025-01-01"
        To: "2025-03-01"
        Period: "1 MONTH"
        Columns:
          A:
            type: temporal
            function: DayOfMonth
        """;
        InputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes(StandardCharsets.UTF_8));
        FormatParser formatParser = new YamlFormatParser();

        Format format = formatParser.parse(inputStream);

        assertThat(format).isNotNull();
        assertThat(format.from()).isEqualTo(LocalDate.of(2025, 1, 1).atStartOfDay(ZoneOffset.UTC).toInstant());
        assertThat(format.to()).isEqualTo(LocalDate.of(2025, 3, 1).atStartOfDay(ZoneOffset.UTC).toInstant());
        assertThat(format.duration()).isEqualTo(Period.ofMonths(1));
        assertThat(format.columns()).hasSize(1);
        assertThat(format.columns().getFirst()).isInstanceOf(Column.Temporal.class);
        assertThat(((Column.Temporal) format.columns().getFirst()).function()).isEqualTo(TemporalFunction.DayOfMonth);
    }

    private static Stream<Arguments> provideInvalidFunctionConfigurations() {
        return Stream.of(
                Arguments.of("temporal", "UnknownTemporalFunction"),
                Arguments.of("numerical", "UnknownNumericalFunction"),
                Arguments.of("categorical", "UnknownCategoricalFunction")
        );
    }
    @ParameterizedTest
    @MethodSource("provideInvalidFunctionConfigurations")
    public void shouldThrowExceptionForUnknownFunctions(String type, String function) throws Exception {
        String invalidYamlContent = String.format("""
            From: "2025-01-01"
            To: "2025-03-01"
            Period: "1 MONTH"
            Columns:
              A:
                type: %s
                function: %s
            """, type, function);
        InputStream inputStream = new ByteArrayInputStream(invalidYamlContent.getBytes(StandardCharsets.UTF_8));
        FormatParser formatParser = new YamlFormatParser();

        assertThatThrownBy(() -> formatParser.parse(inputStream))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown Function");
    }

    @Test
    public void shouldParseYamlWithMultipleColumnTypes() throws Exception {
        String yamlContent = """
            From: "2025-01-01"
            To: "2025-03-01"
            Period: "1 MONTH"
            Columns:
              A:
                type: temporal
                function: DayOfMonth
              B:
                type: numerical
                attribute: temperature
                function: Average
              C:
                type: categorical
                attribute: sky
                function: Mode
            """;

        InputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes(StandardCharsets.UTF_8));
        FormatParser formatParser = new YamlFormatParser();

        Format format = formatParser.parse(inputStream);

        assertThat(format).isNotNull();
        assertThat(format.columns()).hasSize(3);
        assertThat(format.columns().get(0)).isInstanceOf(Column.Temporal.class);
        assertThat(format.columns().get(1)).isInstanceOf(Column.Numerical.class);
        assertThat(format.columns().get(2)).isInstanceOf(Column.Categorical.class);
    }

    @Test
    public void shouldHandleYamlWithoutColumns() throws Exception {
        String yamlContent = """
            From: "2025-01-01"
            To: "2025-03-01"
            Period: "1 MONTH"
            Columns: {}
        """;
        InputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes(StandardCharsets.UTF_8));
        FormatParser formatParser = new YamlFormatParser();

        Format format = formatParser.parse(inputStream);

        assertThat(format).isNotNull();
        assertThat(format.columns()).isEmpty();
    }

    @Test
    public void should_export_tabular_report_with_temporal_numerical_and_categorical_columns() throws Exception {
        String yamlContent = """
            From: "2025-01-01"
            To: "2025-03-01"
            Period: "1 MONTH"

            Columns:
              A:
                type: temporal
                function: DayOfMonth
              B:
                type: temporal
                function: MonthOfYear
              C:
                type: numerical
                attribute: temperature
                function: Average
              D:
                type: numerical
                attribute: temperature
                function: Average
              E:
                type: numerical
                attribute: temperature
                function: First
              F:
                type: numerical
                attribute: temperature
                function: Last
              G:
                type: categorical
                attribute: sky
                function: mode
              H:
                type: temporal
                function: YearMonthDay
        """;
        InputStream inputStream = new ByteArrayInputStream(yamlContent.getBytes(StandardCharsets.UTF_8));
        FormatParser formatParser = new YamlFormatParser();

        Format format = formatParser.parse(inputStream);

        try (SubjectStore store = new SubjectStore(File.createTempFile("xyz", ":patient.oss"))) {
            feed(store);
            SubjectView table = new SubjectView(store, format).normalize(2);
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            table.export(os);
            assertThat(os.toString()).isEqualTo(expected.trim());
        }
    }

    private void feed(SubjectStore store) {
        store.feed(from.plus(10, DAYS), "test")
                .add("temperature", 20)
                .execute();
        store.feed(from.plus(12, DAYS), "test")
                .add("temperature", 28)
                .add("sky", "cloudy")
                .execute();
        store.feed(from.plus(32, DAYS), "test")
                .add("temperature", 18)
                .add("sky", "rain")
                .execute();
    }
}
