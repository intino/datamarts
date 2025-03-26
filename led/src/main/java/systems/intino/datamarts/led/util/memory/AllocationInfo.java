package systems.intino.datamarts.led.util.memory;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import static systems.intino.datamarts.led.util.memory.MemoryUtils.NULL;

public class AllocationInfo implements Comparable<AllocationInfo> {

    private final WeakReference<ByteBuffer> buffer;
    private final StackTraceElement[] allocationStackTrace;
    private final long size;
    private final long timestamp;

    public AllocationInfo(ByteBuffer buffer, long size, StackTraceElement[] stackTrace) {
        this.buffer = new WeakReference<>(buffer);
        this.size = size;
        this.allocationStackTrace = stackTrace;
        this.timestamp = System.currentTimeMillis();
    }

    public boolean isGarbageCollected() {
        return buffer.get() == null;
    }

    public MemoryAddress address() {
        return isGarbageCollected() ? new ModifiableMemoryAddress(NULL) : MemoryAddress.of(buffer.get());
    }

    public long size() {
        return size;
    }

    public float sizeKB() {
        return size / 1024.0f;
    }

    public float sizeMB() {
        return size / 1024.0f / 1024.0f;
    }

    public float sizeGB() {
        return size / 1024.0f / 1024.0f / 1024.0f;
    }

    public StackTraceElement[] allocationStackTrace() {
        return allocationStackTrace;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(AllocationInfo o) {
        return Long.compare(size, o.size);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AllocationInfo that = (AllocationInfo) o;
        return size == that.size &&
                timestamp == that.timestamp &&
                Objects.equals(buffer, that.buffer) &&
                Arrays.equals(allocationStackTrace, that.allocationStackTrace);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(buffer, size, timestamp);
        result = 31 * result + Arrays.hashCode(allocationStackTrace);
        return result;
    }

    @Override
    public String toString() {
        return "AllocationInfo{" +
                "buffer=" + (isGarbageCollected() ? "(DELETED)" : buffer.get()) +
                ",address=" + address().get() +
                ",size=" + (size < 100_000 ? sizeKB() + " KB" : sizeMB() + " MB") +
                ",allocationStackTrace=" + Arrays.stream(allocationStackTrace)
                .map(this::printStackTraceElement).collect(Collectors.joining(", ")) +
                ",dateTime=" + Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime()+
                "}";
    }

    public String toStringPretty() {
        return "AllocationInfo {" +
                "\n buffer=" + (isGarbageCollected() ? "(DELETED)" : buffer.get()) +
                ",\n address=" + address().get() +
                ",\n size=" + (size < 100_000 ? sizeKB() + " KB" : sizeMB() + " MB") +
                ",\n allocationStackTrace=[" + Arrays.stream(allocationStackTrace)
                .map(this::printStackTraceElement).collect(Collectors.joining("\n\t\t\tat ")) + "]" +
                ",\n dateTime=" + Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime()+
                "\n}";
    }

    private String printStackTraceElement(StackTraceElement st) {
        return st.getClassName() + "." + st.getMethodName() + "(" + st.getFileName() + ":" + st.getLineNumber() + ")";
    }
}
