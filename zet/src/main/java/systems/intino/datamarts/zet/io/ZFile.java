package systems.intino.datamarts.zet.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ZFile {
    private final File file;

    public ZFile(String name) {
        this(new File(name));
    }

    public ZFile(File file) {
        this.file = file;
    }

    public long size() throws IOException {
        RandomAccessFile file = new RandomAccessFile(this.file, "r");
        file.seek(file.length() - 8);
        long size = file.readLong();
        file.close();
        return size;
    }


}
