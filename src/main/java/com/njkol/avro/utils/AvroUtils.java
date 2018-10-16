package com.njkol.avro.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Avro utility
 * 
 * @author Nilanjan Sarkar
 *
 */
public class AvroUtils {

	/**
	 * Writes an Avro record to disk
	 * 
	 * The avro data file writer is not thread-safe, so we must synchronize writes
	 * to it
	 * 
	 * @param data
	 * @param typeKey
	 * @param fileName
	 * @throws IOException
	 */
	public static synchronized <T extends SpecificRecordBase> void writeToDisk(List<T> data, Class<T> typeKey,
			String fileName) throws IOException {

		File file = new File(fileName);
		DatumWriter<T> userDatumWriter = new SpecificDatumWriter<T>(typeKey);
		DataFileWriter<T> dataFileWriter = new DataFileWriter<T>(userDatumWriter);
		dataFileWriter.setCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL));
		dataFileWriter.create(data.get(0).getSchema(), file);
		for (T record : data) {
			dataFileWriter.append(record);
		}
		dataFileWriter.close();
	}

	/**
	 * Write generic records to a file on disk
	 * 
	 * @param data
	 * @param schema
	 * @param fileName
	 * @throws IOException
	 */
	public static synchronized void writeGenericToDisk(List<GenericRecord> data, Schema schema, String fileName)
			throws IOException {

		File file = new File(fileName);
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.setCodec(CodecFactory.snappyCodec());
		dataFileWriter.create(schema, file);
		for (GenericRecord record : data) {
			dataFileWriter.append(record);
		}
		dataFileWriter.close();
	}

	/**
	 * Append generic record to an existing avro data file
	 * 
	 * @param data
	 * @param schema
	 * @param fileName
	 * @throws IOException
	 */
	public static synchronized void appendGenericToDisk(List<GenericRecord> data, Schema schema, String fileName)
			throws IOException {

		File file = new File(fileName);
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.setCodec(CodecFactory.snappyCodec());
		dataFileWriter.appendTo(file);
		for (GenericRecord record : data) {
			dataFileWriter.append(record);
		}
		dataFileWriter.close();
	}

	public static synchronized ByteArrayOutputStream writeGenericToMemory(GenericRecord data, Schema schema)
			throws IOException {

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		datumWriter.write(data, encoder);
		encoder.flush();
		out.close();
		return out;
	}

	/**
	 * Read avro records from disk
	 * 
	 * @param typeKey
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static <T extends SpecificRecordBase> List<T> readFromDisk(Class<T> typeKey, String fileName)
			throws IOException {

		File file = new File(fileName);
		DatumReader<T> userDatumReader = new SpecificDatumReader<T>(typeKey);
		DataFileReader<T> dataFileReader = new DataFileReader<T>(file, userDatumReader);
		List<T> data = new ArrayList<T>();
		while (dataFileReader.hasNext()) {
			data.add(dataFileReader.next());
		}
		dataFileReader.close();
		return data;
	}

	/**
	 * Reads a generic record from disk
	 * 
	 * @param schemaFile
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static List<GenericRecord> readGenericFromDisk(String schemaFile, String fileName) throws IOException {

		Schema schema = new Schema.Parser().parse(new File(schemaFile));
		File file = new File(fileName);
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
		List<GenericRecord> data = new ArrayList<GenericRecord>();
		while (dataFileReader.hasNext()) {
			data.add(dataFileReader.next());
		}
		dataFileReader.close();
		return data;
	}

	public static GenericRecord readGenericFromMemory(ByteArrayOutputStream out, String schemaFile) throws IOException {
		Schema schema = new Schema.Parser().parse(new File(schemaFile));
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DecoderFactory decoder = DecoderFactory.get();
		BinaryDecoder bd = decoder.binaryDecoder(out.toByteArray(), null);
		GenericRecord result = datumReader.read(null, bd);
		return result;
	}
}
