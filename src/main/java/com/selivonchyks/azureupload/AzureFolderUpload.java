package com.selivonchyks.azureupload;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	static final String SOURCE_FOLDER_ARG_NAME = "source";

	static final String AZURE_CONNECTION_STRING_PROPERTY_NAME = "connectionString";

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
	private static volatile long uploadedFilesSize = 0;
	private static void uploadFolder(String azureConnectionString, String sourcePath, String targetContainer, int uploadThreadsCount) {
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
			final Collection<File> files = FileUtils.listFiles(sourceFolder, null, true);
			if (files != null && !files.isEmpty()) {
				final int filesCount = files.size();
				logger.debug("Found [{}] files in source folder [{}]", filesCount, sourcePath);

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
									byte[] md5 = DigestUtils.md5(new FileInputStream(file));
									String md5HashBase64 = Base64.encode(md5);

									blobItem = sourceFolderUri.relativize(file.toURI()).getPath();

									long fileUploadStartTime = System.currentTimeMillis();
									CloudBlockBlob blob = container.getBlockBlobReference(blobItem);

									blob.upload(new FileInputStream(file), file.length(), null, blobRequestOptions, operationContext);
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
									uploadedFilesSize += file.length();
									logger.info("Uploaded file [{}] in [{}] ms, totally uploaded [{}] file of [{}]", file, System.currentTimeMillis() - fileUploadStartTime, uploadedFilesCount, filesCount);
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

				logger.info("Finished uploading [{}] files of total size [{}] bytes in [{}] s", uploadedFilesCount, uploadedFilesSize, (System.currentTimeMillis() - startTime) / 1000);
			} else {
				logger.info("Specified source [{}] folder doesn't contain any files");
			}
		} catch (Exception e) {
			logger.warn(String.format("Failed to upload folder [%s] to azure container [%s] using [%d] threads and connection string [%s]", sourcePath, targetContainer, uploadThreadsCount, azureConnectionString), e);
		}
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
					NumberUtils.toInt(commandLine.getOptionValue(THREADS_COUNT_ARG_NAME))
			);
		} catch (ParseException exp) {
			logger.warn(exp.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("ant", options);
		} catch (ConfigurationException e) {
			logger.warn(String.format("Failed to read configuration from %s", configFileLocation), e);
		} catch (Exception e) {
			logger.warn("", e);
		}
	}
}
