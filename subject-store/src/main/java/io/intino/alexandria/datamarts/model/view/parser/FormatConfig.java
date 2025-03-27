package io.intino.alexandria.datamarts.model.view.parser;

import java.util.Map;

public class FormatConfig {
    public String From;
    public String To;
    public String Period;
    public Map<String, ColumnConfig> Columns;

    public static class ColumnConfig {
        public String type;
        public String attribute;   // Para numerical y categorical
        public String function;    // Para temporal/numerical/categorical
        public String formula;     // Para fórmulas
        public String transform;   // Para transforms como Normalize

        @Override
        public String toString() {
            return "ColumnConfig{" +
                    "type='" + type + '\'' +
                    ", attribute='" + attribute + '\'' +
                    ", function='" + function + '\'' +
                    ", formula='" + formula + '\'' +
                    ", transform='" + transform + '\'' +
                    '}';
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("FormatConfig{")
                .append("From='").append(From).append('\'')
                .append(", To='").append(To).append('\'')
                .append(", Period='").append(Period).append('\'')
                .append(", Columns={");

        if (Columns != null) {
            for (Map.Entry<String, ColumnConfig> entry : Columns.entrySet()) {
                sb.append("\n    '").append(entry.getKey()).append("': ").append(entry.getValue()).append(",");
            }
            if (!Columns.isEmpty()) {
                sb.setLength(sb.length() - 1);
                sb.append("\n");
            }
        }
        sb.append("}}");
        return sb.toString();
    }
}
