# azureupload
Upload folder to azure blob storage

# build
mvn clean package

# configuration
Set azure blob storage connection string in file src/main/resources/app.properties.
It's possible to specify path to configuration file via program run argument.
 
# usage
java -jar ./target/azureupload.jar

# run
java -jar ./target/azureupload-jar-with-dependencies.jar --threads <upload threads count> --container <azure blob container> --source <path to folder>

