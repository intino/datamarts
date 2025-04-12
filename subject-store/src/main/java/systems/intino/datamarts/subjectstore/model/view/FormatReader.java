package systems.intino.datamarts.subjectstore.model.view;

import java.io.IOException;

public interface FormatReader {
	Format read() throws IOException;
}
