package systems.intino.alexandria.datamarts.model.view;

import java.io.File;

public class YamlFileFormatLoader implements FormatLoader {
	private final File file;

	public YamlFileFormatLoader(File file) {
		this.file = file;
	}

	@Override
	public Format load() {
		return null;
	}



}
