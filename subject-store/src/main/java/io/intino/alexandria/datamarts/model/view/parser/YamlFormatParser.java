package io.intino.alexandria.datamarts.model.view.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.intino.alexandria.datamarts.model.view.*;
import io.intino.alexandria.datamarts.model.view.functions.CategoricalFunction;
import io.intino.alexandria.datamarts.model.view.functions.NumericalFunction;
import io.intino.alexandria.datamarts.model.view.functions.TemporalFunction;

import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Map;

public class YamlFormatParser implements FormatParser {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Override
    public Format parse(InputStream input) throws Exception {
        FormatConfig config = parseConfig(input);
        Format format = new Format(
                parseDate(config.From),
                parseDate(config.To),
                PeriodParser.parse(config.Period));
        addColumnsToFormat(format, config.Columns);
        return format;
    }

    private FormatConfig parseConfig(InputStream input) throws Exception {
        return mapper.readValue(input, FormatConfig.class);
    }

    private Instant parseDate(String date) {
        return LocalDate.parse(date).atStartOfDay(ZoneOffset.UTC).toInstant();
    }

    private void addColumnsToFormat(Format format, Map<String, FormatConfig.ColumnConfig> columns) {
        for (Map.Entry<String, FormatConfig.ColumnConfig> entry : columns.entrySet()) {
            FormatConfig.ColumnConfig config = entry.getValue();
            format.add(parseColumn(config));
        }
    }

    private Column parseColumn(FormatConfig.ColumnConfig config) {
        return switch (config.type.toLowerCase()) {
            case "temporal" -> parseTemporalColumn(config);
            case "numerical" -> parseNumericalColumn(config);
            case "categorical" -> parseCategoricalColumn(config);
            default -> throw new IllegalArgumentException("Unknown Column type: " + config.type);
        };
    }

    private Column.Temporal parseTemporalColumn(FormatConfig.ColumnConfig config) {
        TemporalFunction function = TemporalFunction.fromString(config.function);
        return new Column.Temporal(function);
    }

    private Column.Numerical parseNumericalColumn(FormatConfig.ColumnConfig config) {
        NumericalFunction function = NumericalFunction.fromString(config.function);
        return new Column.Numerical(config.attribute, function);
    }

    private Column.Categorical parseCategoricalColumn(FormatConfig.ColumnConfig config) {
        CategoricalFunction function = CategoricalFunction.fromString(config.function);
        return new Column.Categorical(config.attribute, function);
    }
}