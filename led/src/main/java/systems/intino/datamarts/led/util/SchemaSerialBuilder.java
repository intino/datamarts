package systems.intino.datamarts.led.util;

import java.util.UUID;

public class SchemaSerialBuilder {

    private StringBuilder builder;
    private UUID serialId;

    public SchemaSerialBuilder(String className) {
        builder = new StringBuilder(className).append("{ ");
    }

    public SchemaSerialBuilder add(String name, String type, int bitIndex, int bitCount) {
        if(builder == null) throw new IllegalStateException("This builder has already been closed");
        builder.append(name).append(" : ").append(type).append('(').append(bitIndex).append(',').append(bitCount).append(") ");
        return this;
    }

    public UUID buildSerialId() {
        if(serialId != null) return serialId;
        final String str = builder.append('}').toString();
        serialId = UUID.nameUUIDFromBytes(str.getBytes());
        builder = null;
        return serialId;
    }
}
