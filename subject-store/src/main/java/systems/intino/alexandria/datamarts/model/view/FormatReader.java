package systems.intino.alexandria.datamarts.model.view;

import java.io.IOException;

public interface FormatReader {
	Format read() throws IOException;
}
