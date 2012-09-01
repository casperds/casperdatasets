package net.casper.io.beans.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import net.casper.data.model.CBuilder;
import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CExporter;
import net.casper.ext.out.CExportCSVString;
import net.casper.io.beans.CBuildFromCollection;
import net.casper.io.beans.CExportBeans;
import net.casper.io.beans.CExportBeansCached;

import org.junit.BeforeClass;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class IoBeansTest {

	private static final String NEWLINE = System.getProperty("line.separator");

	private static final Collection<Person> peopleColl = new LinkedList<Person>();

	@DataPoint
	public static CBuilder builderPK;

	@DataPoint
	public static CBuilder builderNoPK;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		Person p1 = new Person(1, "mike", 'm', 18.25, true);
		Person p2 = new Person(2, "michael", 'm', 28.25, true);
		Person p3 = new Person(3, "peter", 'm', 28.25 + (1.0 / 3.0), false);
		Person p4 = new Person(4, "bob", 'm', 17, true);
		Person p5 = new Person(5, "barbara", 'f', 18.7635120384, false);

		peopleColl.add(p1);
		peopleColl.add(p2);
		peopleColl.add(p3);
		peopleColl.add(p4);
		peopleColl.add(p5);

		// create a casper dataset called "people" from the
		// peopleColl collection using "id" for the primary key
		builderPK = new CBuildFromCollection("people", peopleColl,
				Object.class, "id");

		// create a casper dataset called "people" from the
		// peopleColl collection without a primary key
		builderNoPK = new CBuildFromCollection("people", peopleColl,
				Object.class, null);
	}

	@Theory
	public void testBuildFromCollection(CBuilder builder)
			throws CDataGridException {
		CExporter csvString = new CExportCSVString(false,
				"age,gender,id,name,updated");

		CDataCacheContainer container = new CDataCacheContainer(builder);

		assertEquals(5, container.size());

		String strCSV = (String) container.export(csvString);

		System.out.println(strCSV);

		assertEquals(Person.collToString(peopleColl), strCSV);
	}

	@Theory
	public void testExportBeans(CBuilder builder) throws IOException,
			CDataGridException, InstantiationException, IllegalAccessException {
		CExportBeans<Person> cexportBeans = new CExportBeans<Person>(
				Person.class);

		CDataCacheContainer container = new CDataCacheContainer(builder);

		assertEquals(5, container.size());

		container.export(cexportBeans);

		Collection<Person> beans = cexportBeans.getBeans();

		assertEquals(Person.collToString(peopleColl),
				Person.collToString(beans));

	}

	
	
	
	@Theory	
	public void testExportBeansCached(CBuilder builder) throws IOException,
			CDataGridException, InstantiationException, IllegalAccessException {
		CExportBeansCached<Person> cexportBeans = new CExportBeansCached<Person>(
				Person.class);

		CDataCacheContainer container = new CDataCacheContainer(builder);

		assertEquals(5, container.size());

		container.export(cexportBeans);

		Collection<Person> beans = cexportBeans.getBeans();

		assertEquals(Person.collToString(peopleColl),
				Person.collToString(beans));

	}

	public static class Person {

		private int id;
		private String name;
		private char gender;
		private double age;
		private boolean updated;

		public Person() {

		}

		public Person(int id, String name, char gender, double age,
				boolean updated) {
			super();
			this.id = id;
			this.name = name;
			this.gender = gender;
			this.age = age;
			this.updated = updated;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public char getGender() {
			return gender;
		}

		public void setGender(char gender) {
			this.gender = gender;
		}

		public double getAge() {
			return age;
		}

		public void setAge(double age) {
			this.age = age;
		}

		public boolean isUpdated() {
			return updated;
		}

		public void setUpdated(boolean updated) {
			this.updated = updated;
		}

		@Override
		public String toString() {
			StringBuffer sbuf = new StringBuffer();

			sbuf.append(age).append(", ");
			sbuf.append(gender).append(", ");
			sbuf.append(id).append(", ");
			sbuf.append(name).append(", ");
			sbuf.append(updated).append(NEWLINE);

			return sbuf.toString();
		}

		public static String collToString(Collection<?> coll) {
			StringBuffer sbuf = new StringBuffer();

			for (Object obj : coll) {
				sbuf.append(obj.toString());
			}

			return sbuf.toString();
		}

	}
}
