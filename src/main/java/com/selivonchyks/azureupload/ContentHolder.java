package com.selivonchyks.azureupload;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;

public class ContentHolder {
	private File file;
	private boolean isAllowedInMemoryHandling = false;
	private byte[] data;

	public byte[] getData() {
		return data;
	}
	public void setData(byte[] data) {
		this.data = data;
	}
	public File getFile() {
		return file;
	}
	public void setFile(File file) {
		this.file = file;
	}
	public boolean isAllowedInMemoryHandling() {
		return isAllowedInMemoryHandling;
	}
	public void setAllowedInMemoryHandling(boolean isAllowedInMemoryHandling) {
		this.isAllowedInMemoryHandling = isAllowedInMemoryHandling;
	}
	public long getLength() {
		if (this.isAllowedInMemoryHandling() && this.getData() != null) {
			return this.getData().length;
		} else {
			return this.getFile().length();
		}
	}

	public ContentHolder(File file, boolean isAllowedInMemoryHandling) {
		this.setFile(file);
		this.setAllowedInMemoryHandling(isAllowedInMemoryHandling);
	}

	public synchronized InputStream getInputStream() throws IOException {
		if (this.isAllowedInMemoryHandling()) {
			if (this.getData() == null) {
				this.setData(FileUtils.readFileToByteArray(this.getFile()));
			}
			return new ByteArrayInputStream(this.getData());
		} else {
			return new FileInputStream(this.getFile());
		}
	}
}
