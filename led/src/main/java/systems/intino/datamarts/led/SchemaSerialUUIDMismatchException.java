package systems.intino.datamarts.led;

import java.util.UUID;

public class SchemaSerialUUIDMismatchException extends RuntimeException {

    public SchemaSerialUUIDMismatchException(UUID expected, UUID actual) {
        this(String.format("Source and destination schemas does not have the same UUID: %s vs %s", expected, actual));
    }

    public SchemaSerialUUIDMismatchException(String message) {
        super(message);
    }
}
