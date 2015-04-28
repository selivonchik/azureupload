package com.selivonchyks.azureupload;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.core.Base64;

public class AzureFolderUpload {
	static final Logger logger = LoggerFactory.getLogger(AzureFolderUpload.class);

	static final String DEFAULT_CONFIG_FILE = "app.properties";

	static final String CONFIG_FILE_ARG_NAME = "config";
	static final String THREADS_COUNT_ARG_NAME = "threads";
	static final String TARGET_AZURE_CONTAINER_ARG_NAME = "container";
	static final String TARGET_FOLDER_ARG_NAME = "target";
	static final String SOURCE_FOLDER_ARG_NAME = "source";
	static final String IN_MEMORY_ARG_NAME = "inMemory";
	static final String UPLOAD_LOG_ARG_NAME = "uploadLog";
	static final String SKIP_UPLOADED_ARG_NAME = "skipUploaded";
	static final String FOLDER_READY_MARKER_FILE_ARG_NAME = "folderReadyMarkerFile";

	static final String AZURE_CONNECTION_STRING_PROPERTY_NAME = "connectionString";

	private static CsvMapper mapper;
	private static CsvSchema schema;
	private static ObjectWriter csvObjectWriter;
	private static ObjectReader csvObjectReader;
	private static Collection<UploadedFileLogItem> uploadLogItems;

	@SuppressWarnings("static-access")
	private static Options buildCommandLineOptions() {
		// create the Options
		Options options = new Options();
		options.addOption(
				OptionBuilder
					.withLongOpt(CONFIG_FILE_ARG_NAME)
					.hasArg(true)
					.withDescription(String.format("properties file location, otherwise %s in current folder is used", DEFAULT_CONFIG_FILE))
					.isRequired(false)
					.create()
		);
		options.addOption(
				OptionBuilder
					.withLongOpt(THREADS_COUNT_ARG_NAME)
					.hasArg(true)
					.withDescription("upload threads count")
					.withType(Integer.class)
					.isRequired(true)
					.create()
		);
		options.addOption(
				OptionBuilder
					.withLongOpt(SOURCE_FOLDER_ARG_NAME)
					.hasArg(true)
					.withDescription("source folder to upload")
					.isRequired(true)
					.create()
		);
		options.addOption(
				OptionBuilder
					.withLongOpt(TARGET_AZURE_CONTAINER_ARG_NAME)
					.hasArg(true)
					.withDescription("target azure container")
					.isRequired(true)
					.create()
		);
		options.addOption(
				OptionBuilder
					.withLongOpt(TARGET_FOLDER_ARG_NAME)
					.hasArg(true)
					.withDescription("target folder in azure container")
					.isRequired(false)
					.create()
		);
		options.addOption(
				OptionBuilder
					.withLongOpt(IN_MEMORY_ARG_NAME)
					.hasArg(false)
					.withDescription("allow in memory file handling")
					.isRequired(false)
					.create()
		);
		options.addOption(
				OptionBuilder
					.withLongOpt(UPLOAD_LOG_ARG_NAME)
					.hasArg(true)
					.withDescription("stores uploaded file info like path, size, hash, ... it might be used next time to skip already uploaded files")
					.isRequired(false)
					.create()
		);
		options.addOption(
				OptionBuilder
					.withLongOpt(SKIP_UPLOADED_ARG_NAME)
					.hasArg(true)
					.withDescription("path to previously stored file with upload log, might be the same file as specified by uploadLog argument")
					.isRequired(false)
					.create()
		);
		options.addOption(
				OptionBuilder
					.withLongOpt(FOLDER_READY_MARKER_FILE_ARG_NAME)
					.hasArg(true)
					.withDescription("marker file name, if marker file exists in the folder then this folder is ready for upload")
					.isRequired(false)
					.create()
		);

		return options;
	}

	private static CommandLine parseCommandLine(Options options, String[] args) throws ParseException {
		// create the command line parser
		CommandLineParser parser = new BasicParser();

		// parse the command line arguments
		return parser.parse(options, args);
	}

	private static Configuration readConfiguration(String configFile) throws ConfigurationException {
		logger.info("Reading configuration from {}", configFile);
		return new PropertiesConfiguration(configFile);
	}

	private static volatile int uploadedFilesCount = 0;
	private static volatile int skippedFilesCount = 0;
	private static volatile long uploadedFilesSize = 0;

	private static void uploadFolder(
			final String azureConnectionString, 
			final String sourcePath, 
			final String targetContainer, 
			final String targetFolder,
			final int uploadThreadsCount, 
			final boolean allowInMemoryFileHandling, 
			final String uploadLogFilePath,
			final String skipUploadedFilePath,
			final String folderReadyMarkerFileName
	) {
		try {
			if (StringUtils.isBlank(azureConnectionString)) {
				throw new IllegalArgumentException("Failed to proceed: azure connection string is empty, check properties file");
			}
			if (StringUtils.isBlank(sourcePath)) {
				throw new IllegalArgumentException("Failed to proceed: source path is empty");
			}
			if (StringUtils.isBlank(targetContainer)) {
				throw new IllegalArgumentException("Failed to proceed: target azure container is empty");
			}
			if (uploadThreadsCount < 1) {
				throw new IllegalArgumentException(String.format("Failed to proceed: specified upload threads count %d is less than 1", uploadThreadsCount));
			}

			long startTime = System.currentTimeMillis();
			final File sourceFolder = new File(sourcePath);
			final URI sourceFolderUri = sourceFolder.toURI();
			final Collection<File> files = listFiles(sourceFolder, folderReadyMarkerFileName);
			if (files != null && !files.isEmpty()) {
				final int filesCount = files.size();
				logger.debug("Found [{}] files in source folder [{}]", filesCount, sourcePath);

				prepareUploadLogSchema();
				readUploadLog(skipUploadedFilePath);

				try (Writer writer = prepareUploadLogWriter(uploadLogFilePath)) {
					ExecutorService exec = Executors.newFixedThreadPool(uploadThreadsCount);
					try {
						CloudStorageAccount storageAccount = CloudStorageAccount.parse(azureConnectionString);
						CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
						final CloudBlobContainer container = blobClient.getContainerReference(targetContainer);
						container.createIfNotExists();
	
						final BlobRequestOptions blobRequestOptions = new BlobRequestOptions();
						blobRequestOptions.setUseTransactionalContentMD5(true);
						final OperationContext operationContext = new OperationContext();
						operationContext.setLogger(logger);
						// operationContext.setLoggingEnabled(true);
	
						logger.info("Starting uploading folder [{}] to azure container [{}] using [{}] threads ...", sourceFolder, container.getUri(), uploadThreadsCount);
	
						for (final File file : files) {
							exec.submit(new Runnable() {
								@Override
								public void run() {
									String blobItem = null;
									try {
										String filePath = file.getAbsolutePath();
										long fileSize = file.length();
										long lastModificationDate = file.lastModified();

										if (checkFileHasBeenAlreadyUploaded(filePath, fileSize, lastModificationDate, null)) {
											++skippedFilesCount;
											logger.info("Skipping file [{}], it has been already uploaded", filePath);
											return;
										}

										ContentHolder contentHolder = new ContentHolder(file, allowInMemoryFileHandling);
										byte[] md5 = null;
										try (InputStream is = contentHolder.getInputStream()) {
											md5 = DigestUtils.md5(new FileInputStream(file));
										}
										String md5HashBase64 = Base64.encode(md5);

										if (checkFileHasBeenAlreadyUploaded(filePath, fileSize, lastModificationDate, md5)) {
											++skippedFilesCount;
											logger.info("Skipping file [{}], it has been already uploaded", filePath);
											return;
										}

										blobItem = sourceFolderUri.relativize(file.toURI()).getPath();
										if (StringUtils.isNotBlank(targetFolder)) {
											blobItem = FilenameUtils.normalize(String.format("%s/%s", targetFolder, blobItem), true);
										}
	
										long fileUploadStartTime = System.currentTimeMillis();
										CloudBlockBlob blob = container.getBlockBlobReference(blobItem);
	
										try (InputStream is = contentHolder.getInputStream()) {
											blob.upload(is, fileSize, null, blobRequestOptions, operationContext);
										}
										String uploadedFileHash = blob.getProperties().getContentMD5();
										if (!StringUtils.equals(md5HashBase64, uploadedFileHash)) {
											try {
												blob.deleteIfExists();
											} catch (Exception e) {
												logger.info(String.format("Failed to delete broken blob [%s]", blob.getUri().toString()), e);
											}
											throw new IOException(String.format("Uploaded file [%s] has wrong hash [%s] but expected [%s]", blob.getUri().toString(), uploadedFileHash, md5HashBase64));
										}
	
										uploadedFilesCount++;
										uploadedFilesSize += fileSize;
										logger.info("Uploaded file [{}] to [{}] in [{}] ms, totally uploaded [{}] files of [{}] ([{}] skipped)", file, blob.getUri(), System.currentTimeMillis() - fileUploadStartTime, uploadedFilesCount, filesCount, skippedFilesCount);
	
										logUpload(writer, filePath, fileSize, md5, System.currentTimeMillis(), lastModificationDate);
									} catch (IOException | URISyntaxException | StorageException e) {
										logger.warn(String.format("Failed to upload file [%s]", file), e);
									}
								}
							});
						}
					} catch (InvalidKeyException | URISyntaxException | StorageException e) {
						logger.warn("Upload failed", e);
						System.exit(1);
					} finally {
						exec.shutdown();
					}
	
					try {
						exec.awaitTermination(31, TimeUnit.DAYS);
					} catch (InterruptedException ex) {
						logger.error("Upload failed", ex);
						System.exit(1);
					}
				}
				logger.info("Finished uploading [{}] files (+ [{}] skipped) of total size [{}] bytes in [{}] s", uploadedFilesCount, skippedFilesCount, uploadedFilesSize, (System.currentTimeMillis() - startTime) / 1000);
			} else {
				logger.info("Specified source [{}] folder doesn't contain any files");
			}
		} catch (Exception e) {
			logger.warn(String.format("Failed to upload folder [%s] to azure container [%s] using [%d] threads and connection string [%s]", sourcePath, targetContainer, uploadThreadsCount, azureConnectionString), e);
		}
	}
	
	private static Collection<File> listFiles(final File directory, final String markerFileName) {
		Collection<File> files = null;
		final AtomicInteger skippedFilesCount = new AtomicInteger(0);
		long startTime = System.currentTimeMillis();
		if (StringUtils.isBlank(markerFileName)) {
			files = FileUtils.listFiles(directory, null, true);
		} else {
			files = FileUtils.listFiles(
					directory, 
					new IOFileFilter() {
						@Override
						public boolean accept(File dir, String name) {
							return true;
						}
						
						@Override
						public boolean accept(File file) {
							if (file.isFile()) {
								File parentFolder = file;
								do {
									parentFolder = parentFolder.getParentFile();
									File markerFile = new File(parentFolder, markerFileName);
									if (markerFile.exists()) {
										return true;
									}
								} while (parentFolder != null && !parentFolder.equals(directory));
								skippedFilesCount.incrementAndGet();
							}
							return false;
						}
					},
					TrueFileFilter.INSTANCE
			);
		}
		logger.info("Found [{}] files in folder [{}] using marker file [{}] (skipped [{}] files), it took [{}] ms", files != null ? files.size() : 0, directory, markerFileName, skippedFilesCount.intValue(), System.currentTimeMillis() - startTime);
		return files;
	}

	private static void prepareUploadLogSchema() {
		mapper = new CsvMapper();
		schema = mapper.schemaFor(UploadedFileLogItem.class).withoutHeader();
		csvObjectWriter = mapper.writer(schema);
		csvObjectReader = mapper.reader(UploadedFileLogItem.class).with(schema);
	}

	private static void readUploadLog(String path) {
		if (StringUtils.isNotBlank(path)) {
			long startTime = System.currentTimeMillis();
			try {
				File uploadLogFile = new File(path);
				if (uploadLogFile.exists()) {
					MappingIterator<UploadedFileLogItem> it = csvObjectReader.readValues(uploadLogFile);
					uploadLogItems = Collections.synchronizedList(it.readAll());
					logger.info("Read [{}] upload log items from [{}] in [{}] ms", uploadLogItems.size(), uploadLogFile, System.currentTimeMillis() - startTime);
				}
			} catch (Exception e) {
				logger.warn(String.format("Failed to read upload log file [%s]", path), e);
			}
		}		
	}
	
	private static Writer prepareUploadLogWriter(String path) {
		if (StringUtils.isNotBlank(path)) {
			File uploadLogFile = new File(path);
			if (uploadLogFile.exists()) {
				logger.info("Upload log file [{}] already exists, appending it", uploadLogFile);
			}
			try {
				return Files.newBufferedWriter(uploadLogFile.toPath(), Charsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
			} catch (IOException e) {
				logger.warn(String.format("Failed to create writer for upload log file [%s]", path), e);
			}			
		}
		return null;
	}

	private static void logUpload(Writer writer, String filePath, long size, byte[] hash, long uploadDate, long lastModification) {
		if (writer == null) {
			return;
		}
		UploadedFileLogItem logItem = new UploadedFileLogItem();
		logItem.setHash(Hex.encodeHexString(hash));
		logItem.setLast_modification(lastModification);
		logItem.setPath(filePath);
		logItem.setSize(size);
		logItem.setUploaded(uploadDate);
		
		try {
			String logItemString = csvObjectWriter.writeValueAsString(logItem);
			IOUtils.write(logItemString, writer);
		} catch (IOException e) {
			logger.info(String.format("Failed to save upload log item for file [%s]", filePath), e);
		}
	}

	private static boolean checkFileHasBeenAlreadyUploaded(final String path, final long size, final long lastModificationDate, final byte[] hash) {
		if (uploadLogItems != null && !uploadLogItems.isEmpty()) {
			String hashHexTmp = null;
			if (hash != null && hash.length > 0) {
				hashHexTmp = Hex.encodeHexString(hash);
			}
			final String hashHex = hashHexTmp;
			UploadedFileLogItem uploadedFileLogItem = CollectionUtils.find(uploadLogItems, new Predicate<UploadedFileLogItem>() {
				@Override
				public boolean evaluate(UploadedFileLogItem item) {
					if (StringUtils.isNotBlank(hashHex)) {
						if (StringUtils.equalsIgnoreCase(path, item.getPath()) &&
							StringUtils.equals(hashHex, item.getHash()) &&
							size == item.getSize()
						) {
							return true;
						}
					} else {
						if (StringUtils.equalsIgnoreCase(path, item.getPath()) &&
							lastModificationDate == item.getLast_modification() &&
							size == item.getSize()
						) {
							return true;
						}
					}
					return false;
				}
			});
			if (uploadedFileLogItem != null) {
				logger.debug("File [{}] has been already uploaded on [{}]", path, new Date(uploadedFileLogItem.getUploaded()));
				return true;
			}
		}

		return false;
	}

	public static void main(String[] args) {
		String configFileLocation = DEFAULT_CONFIG_FILE;
		Options options = buildCommandLineOptions();
		try {
			CommandLine commandLine = parseCommandLine(options, args);
			if (commandLine.hasOption(CONFIG_FILE_ARG_NAME)) {
				String tmp = commandLine.getOptionValue(CONFIG_FILE_ARG_NAME);
				if (StringUtils.isNotBlank(tmp)) {
					configFileLocation = tmp;
				}
			}

			Configuration configuration = readConfiguration(configFileLocation);
			String azureConnectionString = configuration.getString(AZURE_CONNECTION_STRING_PROPERTY_NAME);
			uploadFolder(
					azureConnectionString, 
					commandLine.getOptionValue(SOURCE_FOLDER_ARG_NAME),
					commandLine.getOptionValue(TARGET_AZURE_CONTAINER_ARG_NAME),
					commandLine.getOptionValue(TARGET_FOLDER_ARG_NAME),
					NumberUtils.toInt(commandLine.getOptionValue(THREADS_COUNT_ARG_NAME)),
					commandLine.hasOption(IN_MEMORY_ARG_NAME),
					commandLine.getOptionValue(UPLOAD_LOG_ARG_NAME),
					commandLine.getOptionValue(SKIP_UPLOADED_ARG_NAME),
					commandLine.getOptionValue(FOLDER_READY_MARKER_FILE_ARG_NAME)
			);
		} catch (ParseException exp) {
			logger.warn(exp.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("azureupload", options);
		} catch (ConfigurationException e) {
			logger.warn(String.format("Failed to read configuration from %s", configFileLocation), e);
		} catch (Exception e) {
			logger.warn("", e);
		}
	}
}
