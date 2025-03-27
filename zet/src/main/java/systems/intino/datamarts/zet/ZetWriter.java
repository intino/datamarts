package systems.intino.datamarts.zet;

import io.intino.alexandria.logger.Logger;
import systems.intino.datamarts.zet.io.ZOutputStream;

import java.io.*;
import java.util.List;
import java.util.stream.Stream;

public class ZetWriter {
	private final OutputStream stream;

	public ZetWriter(File file) {
		file.getParentFile().mkdirs();
		stream = fileOutputStream(file);
	}

	public ZetWriter(OutputStream stream) {
		this.stream = stream;
	}

	public void write(long... data) {
		write(new ZetReader(data));
	}

	public void write(List<Long> messages) {
		write(new ZetReader(messages));
	}

	public void write(Stream<Long> stream) {
		write(new ZetReader(stream));
	}

	public void write(ZetStream stream) {
		try (ZOutputStream outputStream = zOutputStream()) {
			while (stream.hasNext()) outputStream.writeLong(stream.next());
		} catch (IOException e) {
			Logger.error(e);
		}
	}

	private ZOutputStream zOutputStream() {
		return new ZOutputStream(new BufferedOutputStream(stream));
	}

	private OutputStream fileOutputStream(File file) {
		try{
			return new FileOutputStream(file);
		}catch (IOException e){
			Logger.error(e);
			return new ByteArrayOutputStream();
		}
	}

}