package systems.intino.datamarts.subjectstore.subjects.led.util.memory;

import static systems.intino.datamarts.subjectstore.subjects.led.util.memory.MemoryUtils.NULL;

public final class NativePointerCleaner implements Runnable {

    private final ModifiableMemoryAddress address;

    public NativePointerCleaner(ModifiableMemoryAddress address) {
        this.address = address;
    }

    @Override
    public void run() {
        MemoryUtils.free(address.get());
        address.set(NULL);
    }
}
