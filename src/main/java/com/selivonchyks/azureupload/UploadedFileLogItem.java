package com.selivonchyks.azureupload;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({
	UploadedFileLogItem.UPLOADED_FILE_PATH, 
	UploadedFileLogItem.FILE_SIZE,
	UploadedFileLogItem.UPLOAD_DATE,
	UploadedFileLogItem.LAST_MODIFICATION,
	UploadedFileLogItem.HASH
})
public class UploadedFileLogItem implements Serializable {
	private static final long serialVersionUID = 8011072622156570566L;

	public static final String UPLOADED_FILE_PATH = "path";
	public static final String FILE_SIZE = "size";
	public static final String LAST_MODIFICATION = "last_modification";
	public static final String UPLOAD_DATE = "uploaded";
	public static final String HASH = "hash";

	@JsonProperty(value = UPLOADED_FILE_PATH, required = true)
	private String path;

	@JsonProperty(value = FILE_SIZE, required = true)
	private long size;

	@JsonProperty(value = LAST_MODIFICATION, required = true)
	private long last_modification;

	@JsonProperty(value = UPLOAD_DATE, required = true)
	private long uploaded;

	@JsonProperty(value = HASH, required = true)
	private String hash;

	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public long getSize() {
		return size;
	}
	public void setSize(long size) {
		this.size = size;
	}
	public long getLast_modification() {
		return last_modification;
	}
	public void setLast_modification(long last_modification) {
		this.last_modification = last_modification;
	}
	public long getUploaded() {
		return uploaded;
	}
	public void setUploaded(long uploaded) {
		this.uploaded = uploaded;
	}
	public String getHash() {
		return hash;
	}
	public void setHash(String hash) {
		this.hash = hash;
	}
}
