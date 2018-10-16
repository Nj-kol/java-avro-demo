package com.njkol.avro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.njkol.avro.models.User;
import com.njkol.avro.models.User.Builder;
import com.njkol.avro.utils.AvroUtils;

/**
 * Demo showing Java Avro API
 * 
 * @author Nilanjan Sarkar
 *
 */
public class AvroTester {

	public static final String schemaName = "./src/main/resources/user_v1.avsc";

	public static void main(String[] args) throws IOException {
		
		// writeTest(schemaName);
		// readTest();
		// writeGenericTest(schemaName);
		// appendGenericTest(schemaName);
		// readGenericTest(schemaName);
		avroInMemoryTest(schemaName);
		System.exit(0);
	}

	private static void writeTest(String schemaName) throws IOException {

		List<User> users = new ArrayList<User>();

		Builder usrBuildr = User.newBuilder();
		usrBuildr = usrBuildr.setId(1);
		usrBuildr = usrBuildr.setUsername("Nj-Kol");
		usrBuildr = usrBuildr.setEmailAddress("nilanjan.sarkar100@gmail.com");
		usrBuildr = usrBuildr.setPhoneNumber("9031871234");
		usrBuildr = usrBuildr.setFirstName("Nilanjan");
		usrBuildr = usrBuildr.setMiddleName("Kamalesh");
		usrBuildr = usrBuildr.setLastName("Sarkar");
		usrBuildr = usrBuildr.setSex("M");

		User usr = usrBuildr.build();

		users.add(usr);
		AvroUtils.writeToDisk(users, User.class, schemaName);
		System.out.println("Data written successfully to Avro file!!");
	}

	private static void writeGenericTest(String schemaName) throws IOException {

		List<GenericRecord> users = new ArrayList<GenericRecord>();

		Schema schema = new Schema.Parser().parse(new File(schemaName));

		GenericRecord emp1 = new GenericData.Record(schema);
		emp1.put("id", 1);
		emp1.put("username", "deep_dey");
		emp1.put("email_address", "deep.de@gmail.com");
		emp1.put("phone_number", "7892198879");
		emp1.put("first_name", "Deep");
		emp1.put("middle_name", "Chandra");
		emp1.put("last_name", "Dey");
		emp1.put("sex", "M");

		GenericRecord emp2 = new GenericData.Record(schema);
		emp2.put("id", 2);
		emp2.put("username", "Nj-Kol");
		emp2.put("email_address", "nilanjan.sarkar100@gmail.com");
		emp2.put("phone_number", "9031871234");
		emp2.put("first_name", "Nilanjan");
		emp2.put("middle_name", "Kamalesh");
		emp2.put("last_name", "Sarkar");
		emp2.put("sex", "M");

		users.add(emp1);
		users.add(emp2);

		AvroUtils.writeGenericToDisk(users, schema, "user_generic.avro");
		System.out.println("Data written successfully to Avro file!!");
	}

	private static void appendGenericTest(String schemaName) throws IOException {

		List<GenericRecord> users = new ArrayList<GenericRecord>();

		Schema schema = new Schema.Parser().parse(new File(schemaName));

		GenericRecord emp1 = new GenericData.Record(schema);
		emp1.put("id", 3);
		emp1.put("username", "moloy_das");
		emp1.put("email_address", "moloy.das@gmail.com");
		emp1.put("phone_number", "9892198879");
		emp1.put("first_name", "Moloy");
		emp1.put("middle_name", "Kumar");
		emp1.put("last_name", "Das");
		emp1.put("sex", "M");

		GenericRecord emp2 = new GenericData.Record(schema);
		emp2.put("id", 4);
		emp2.put("username", "arpit_cool");
		emp2.put("email_address", "arpit.cool100@gmail.com");
		emp2.put("phone_number", "9031871234");
		emp2.put("first_name", "Arpit");
		emp2.put("middle_name", " ");
		emp2.put("last_name", "Aggarwaal");
		emp2.put("sex", "M");

		users.add(emp1);
		users.add(emp2);

		AvroUtils.appendGenericToDisk(users, schema, "user_generic.avro");
		System.out.println("Data appended successfully to Avro file!!");
	}

	private static void avroInMemoryTest(String schemaName) throws IOException {

		Schema schema = new Schema.Parser().parse(new File(schemaName));

		GenericRecord emp2 = new GenericData.Record(schema);
		emp2.put("id", 4);
		emp2.put("username", "arpit_cool");
		emp2.put("email_address", "arpit.cool100@gmail.com");
		emp2.put("phone_number", "9031871234");
		emp2.put("first_name", "Arpit");
		emp2.put("middle_name", " ");
		emp2.put("last_name", "Aggarwaal");
		emp2.put("sex", "M");

		ByteArrayOutputStream bf = AvroUtils.writeGenericToMemory(emp2, schema);
		GenericRecord rec = AvroUtils.readGenericFromMemory(bf, schemaName);
		System.out.println(rec);
	}

	private static void readTest() throws IOException {
		List<User> data = AvroUtils.readFromDisk(User.class, "user.avro");
		for (User u : data) {
			System.out.println(u);
		}
	}

	private static void readGenericTest(String schemaName) throws IOException {
		List<GenericRecord> data = AvroUtils.readGenericFromDisk(schemaName, "user_generic.avro");
		for (GenericRecord rec : data) {
			System.out.println(rec);
		}
	}
}
