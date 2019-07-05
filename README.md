# Java Avro API

## Dependencies

```
<dependency>
	<groupId>org.apache.avro</groupId>
	<artifactId>avro-compiler</artifactId>
	<version>${avro.version}</version>
</dependency>
```

## Schema

* Define a schema file with extension .avsc

* For example ( user.avsc )

```
{"namespace": "com.njkol.avro.models",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "id", "type": "int"},
     {"name": "username",  "type": ["string", "null"]},
     {"name": "email_address",  "type": ["string", "null"]},
     {"name": "phone_number",  "type": ["string", "null"]},
     {"name": "first_name",  "type": ["string", "null"]},
     {"name": "middle_name",  "type": ["string", "null"]},
     {"name": "last_name",  "type": ["string", "null"]},
     {"name": "sex",  "type": ["string", "null"]}
 ]
}
```

# With Code generation

* Add the avro maven plugin to the build section of pom.xml

```
	<build>
		<plugins>
			<plugin>
				<groupId>org.apache.avro</groupId>
				<artifactId>avro-maven-plugin</artifactId>
				<version>${avro.version}</version>
				<executions>
					<execution>
						<phase>generate-sources</phase>
						<goals>
							<goal>schema</goal>
						</goals>
						<configuration>
							<sourceDirectory>${project.basedir}/src/main/resources/</sourceDirectory>
							<outputDirectory>${project.basedir}/src/main/java/</outputDirectory>
						</configuration>
					</execution>
				</executions>
			</plugin>
			<plugin>
				<groupId>org.apache.maven.plugins</groupId>
				<artifactId>maven-compiler-plugin</artifactId>
				<configuration>
					<source>1.6</source>
					<target>1.6</target>
				</configuration>
			</plugin>
		</plugins>
	</build>
```

* Now, if you do a maven build ( compile, or install ), the Java POJO
  objects will be generated from the schema specified in sourceDirectory
  ( src/main/resources ) to the value namespace attribute specified in
  the .avsc file ( com.njkol.avro.models )

## Serializing

* After the code has been generated for the schema, you can write the value objects to disk

  **Main Classes**

  *SpecificDatumWriter*
  � Java I-O Class to write data of a schema.
  - It implements the base interface DatumWriter.
  - DatumWriter converts Java objects into an in-memory serialized format

  *DataFileWriter*
   � Stores a sequence of data conforming to a schema in a file
   - The schema is stored in the file with the data
   - Each datum in a file is of the same schema
   - Data is written with a DatumWriter
   - Data is grouped into blocks
   - A synchronization marker is written between blocks, so that files can be split
   - Blocks can be compressed
   - Extensible metadata is stored at the end of the file
   - Files may be appended to

  **Sample Code**

	```
	Builder usrBuildr = User.newBuilder();
	usrBuildr = usrBuildr.setId(2);
	usrBuildr = usrBuildr.setUsername("Nj-Kol");
	usrBuildr = usrBuildr.setEmailAddress("nilanjan.sarkar100@gmail.com");
	usrBuildr = usrBuildr.setPhoneNumber("9031871234");
	usrBuildr = usrBuildr.setFirstName("Nilanjan");
	usrBuildr = usrBuildr.setMiddleName(" ");
	usrBuildr = usrBuildr.setLastName("Sarkar");
	usrBuildr = usrBuildr.setSex("M");

	User usr = usrBuildr.build();

	// Serialize usr to disk
	File file = new File("user.avro");
	DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
	DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
	dataFileWriter.create(usr.getSchema(), file);
	dataFileWriter.append(usr);
	dataFileWriter.close();
	```

## Deserializing

  **Main Classes**

 *DataFileReader*      � Provides random access to files written with DataFileWriter
 *SpecificDatumReader* � Reads data of a schema. It implements DatumReader interface

  **Sample Code**

    ```
      File file = new File("user.avro");		
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
		DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader);
		User usr = null;
		while (dataFileReader.hasNext()) {
			usr = dataFileReader.next();			
			System.out.println(usr);
		}
	```
			
# Without Code generation

* As avro data files contain schema along with the actual data blocks,
  we can always read a serialized item regardless of whether we know
  the schema ahead of time or not

## Serializing

**Main Classes**

*Schema*         - In-memory representation of Schema
*GenericRecord*  - A record object representing serialized data
*DataFileWriter* - Writes the avro data file on disk.

**Sample Code**

		Schema schema = new Schema.Parser().parse(new File("user.avsc"));

		GenericRecord user1 = new GenericData.Record(schema);
		user1.put("id", 2);
		user1.put("username", "Nj-Kol");
		user1.put("email_address", "nilanjan.sarkar100@gmail.com");
		user1.put("phone_number", "9031871234");
		user1.put("first_name", "Nilanjan");
		user1.put("middle_name", " ");
		user1.put("last_name", "Sarkar");
		user1.put("sex", "M");

	   File file = new File("users_generic.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, file);
		dataFileWriter.append(user1);
		dataFileWriter.close();

## Deserializing

**Main Classes**

*GenericDatumReader* - Converts in-memory serialized items into GenericRecords.
*DataFileReader*     - Reads the avro data file on disk.

**Sample Code**

      Schema schema = new Schema.Parser().parse(new File("user.avsc"));
		GenericRecord emp = new GenericData.Record(schema);
		File file = new File("users_generic.avro");
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
		while (dataFileReader.hasNext()) {
			emp = dataFileReader.next();
			System.out.println(emp);
		}

# Append data to an existing File

* You can append data to an existing Avro data file
* To do so, you have to call *appendTo* method on a dataFileWriter.

		dataFileWriter.appendTo(file);

* Note - This has to be called only once and not on every record.
         Doing so will in fact lead to throwing an exception

**Sample Code**

        File file = new File(fileName);
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.setCodec(CodecFactory.snappyCodec());
		dataFileWriter.appendTo(file);
		for (GenericRecord record : data) {
			dataFileWriter.append(record);
		}
		dataFileWriter.close();

# Data Compression

* You can compression the data written to Avro file
* The codes available out-of-box are :
  *	null
  *	deflate
  *	snappy
  * bzip2

 **Example**

     dataFileWriter.setCodec(CodecFactory.snappyCodec());));

# Avro In-memory

* Instead of writing data to file, you can also write them to memory

**Writing Avro data to memory**

	    ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		datumWriter.write(data, encoder);
		encoder.flush();
		out.close();

**Reading Avro data from memory**

        Schema schema = new Schema.Parser().parse(new File(schemaFile));
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DecoderFactory decoder = DecoderFactory.get();
		BinaryDecoder bd = decoder.binaryDecoder(out.toByteArray(), null);
		GenericRecord result = datumReader.read(null, bd);

## Use case

* A use case of this could be to encode Kafka message as avro binary
  data. The producer can write data in avro format and write them
  as binary data in a ProducerRecord  

		ByteArrayOutputStream record = new ByteArrayOutputStream();
		BinaryEncoder binaryEncoder = avroEncoderFactory.binaryEncoder(record, null);
		avroWriter.write(operatorAction, binaryEncoder);
		binaryEncoder.flush();
		ProducerRecord  producerRecord = new ProducerRecord(topic, key.getBytes(), record.toByteArray());

References
==========
http://hadooptutorial.info/avro-serializing-and-deserializing-example-java-api

https://www.baeldung.com/java-apache-avro
