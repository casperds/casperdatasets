package net.casper.io.file.tests;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import net.casper.data.model.CBuilder;
import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CDataRowSet;
import net.casper.io.CBuildFromTableReader;

import org.junit.Test;
import org.omancode.rmt.cellreader.CellReader;
import org.omancode.rmt.cellreader.CellReaders;
import org.omancode.rmt.tablereader.AbstractTableReader;
import org.omancode.rmt.tablereader.file.DelimitedFileReader;
import org.omancode.rmt.tablereader.file.ExcelFileReader;

public class CBuildFromTableReaderTest {

	public static final String TEST_DIR = "resources/";
	public static final Double EPSILON = 1e-15;

	private File xlspeople = new File(TEST_DIR + "xls_people.xls");
	private File xlsMissingValues = new File(TEST_DIR
			+ "xls_missing_values.xls");

	private File patientsCSV = new File(TEST_DIR + "patients.csv");
	private String[] columnNames = new String[] { "refnum", "sex", "crefnum",
			"question", "age", "weight" };
	private Class<?>[] columnTypes = new Class<?>[] { Integer.class,
			Character.class, Integer.class, Integer.class, Integer.class,
			Double.class };
	private CellReader<?>[] columnOptionalReaders = new CellReader<?>[] {
			CellReaders.OPTIONAL_INTEGER, CellReaders.CHARACTER,
			CellReaders.OPTIONAL_INTEGER, CellReaders.OPTIONAL_INTEGER,
			CellReaders.OPTIONAL_INTEGER, CellReaders.OPTIONAL_DOUBLE };

	private final AbstractTableReader patientsSpecified = new DelimitedFileReader(
			patientsCSV, columnNames, columnOptionalReaders, null);
	private final AbstractTableReader patientsUnspecified = new DelimitedFileReader(
			patientsCSV);

	private String[] header = new String[] { "id", "name", "gender", "age",
			"updated" };
	private CellReader<?>[] readers = new CellReader<?>[] {
			CellReaders.INTEGER, CellReaders.STRING, CellReaders.CHARACTER,
			CellReaders.DOUBLE, CellReaders.BOOLEAN };
	private Class<?>[] peopleTypes = new Class[] { Integer.class, String.class,
			Character.class, Double.class, Boolean.class };

	private final AbstractTableReader xlsSpecified = new ExcelFileReader(
			new File(TEST_DIR + "xls_people.xls"), header, readers);
	private final AbstractTableReader xlsUnspecified = new ExcelFileReader(
			xlspeople);
	private final AbstractTableReader xlsMissingValuesUnspecified = new ExcelFileReader(
			xlsMissingValues);

	public CBuildFromTableReaderTest() throws IOException {

	}

	@Test
	public void testPatientsBuildColumnsUnspecified() throws IOException,
			CDataGridException {
		// load base file into casper container
		CBuilder builder = new CBuildFromTableReader(patientsUnspecified,
				"refnum,crefnum");

		CDataCacheContainer container = new CDataCacheContainer(builder);
		System.out.println(container.getAll());
		assertEquals(4, container.size());
	}

	@Test
	public void testPatientsRowsUnspecified() throws IOException,
			CDataGridException {

		String[] row1 = new String[] { "1", "M", "", "2", "45", "841.9098462" };
		String[] row2 = new String[] { "2", "F", "", "1", "33", "7.587573231" };
		String[] row3 = new String[] { "", "F", "1000", "", "4", "1696.537051" };
		String[] row4 = new String[] { "", "M", "2000", "", "5",
				"0.123456789012345" };

		CBuilder builder = new CBuildFromTableReader(patientsUnspecified,
				"refnum,crefnum");

		assertArrayEquals(columnNames, builder.getColumnNames());

		assertArrayEquals(new Class<?>[] { Object.class, Object.class,
				Object.class, Object.class, Object.class, Object.class },
				builder.getColumnTypes());

		assertArrayEquals(row1, builder.readRow());
		assertArrayEquals(row2, builder.readRow());
		assertArrayEquals(row3, builder.readRow());
		assertArrayEquals(row4, builder.readRow());
		assertArrayEquals(null, builder.readRow());

	}

	@Test
	public void testPatientsBuildSpecifiedColumnsWithMissingValues()
			throws IOException, CDataGridException {
		CBuilder builder = new CBuildFromTableReader(patientsSpecified,
				"refnum,crefnum");

		CDataCacheContainer container = new CDataCacheContainer(builder);
		container.getAll();
		assertEquals(4, container.size());
	}

	@Test
	public void testPatientsSpecifiedRowsWithMissingValues()
			throws IOException, CDataGridException {
		Double weight1 = Double.valueOf(841.9098462);
		Double weight2 = Double.valueOf(7.587573231);
		Double weight3 = Double.valueOf(1696.537051);
		Double weight4 = Double.valueOf(0.123456789012345);

		Object[] erow1 = new Object[] { Integer.valueOf(1),
				Character.valueOf('M'),
				Integer.valueOf(CellReaders.MISSING_VALUE_INTEGER),
				Integer.valueOf(2), Integer.valueOf(45), weight1 };
		Object[] erow2 = new Object[] { Integer.valueOf(2),
				Character.valueOf('F'),
				Integer.valueOf(CellReaders.MISSING_VALUE_INTEGER),
				Integer.valueOf(1), Integer.valueOf(33), weight2 };
		Object[] erow3 = new Object[] {
				Integer.valueOf(CellReaders.MISSING_VALUE_INTEGER),
				Character.valueOf('F'), Integer.valueOf(1000),
				Integer.valueOf(CellReaders.MISSING_VALUE_INTEGER),
				Integer.valueOf(4), weight3 };
		Object[] erow4 = new Object[] {
				Integer.valueOf(CellReaders.MISSING_VALUE_INTEGER),
				Character.valueOf('M'), Integer.valueOf(2000),
				Integer.valueOf(CellReaders.MISSING_VALUE_INTEGER),
				Integer.valueOf(5), weight4 };

		CBuilder builder = new CBuildFromTableReader(patientsSpecified,
				"refnum,crefnum");

		builder.open();

		assertArrayEquals(columnNames, builder.getColumnNames());

		assertArrayEquals(columnTypes, builder.getColumnTypes());

		Object[] arow1 = builder.readRow();
		Object[] arow2 = builder.readRow();
		Object[] arow3 = builder.readRow();
		Object[] arow4 = builder.readRow();

		assertArrayEquals(erow1, arow1);
		assertEquals(weight1, (Double) arow1[5], EPSILON);

		assertArrayEquals(erow2, arow2);
		assertEquals(weight2, (Double) arow2[5], EPSILON);

		assertArrayEquals(erow3, arow3);
		assertEquals(weight3, (Double) arow3[5], EPSILON);

		assertArrayEquals(erow4, arow4);
		assertEquals(weight4, (Double) arow4[5], EPSILON);

		assertArrayEquals(null, builder.readRow());
	}

	@Test
	public void testPKNotSpecified() throws IOException {

		try {
			CBuildFromTableReader bxls = new CBuildFromTableReader(
					xlsSpecified, "");
			fail("IOException not genereated");
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}

	}

	@Test
	public void testPKDoesntExist() throws IOException {

		try {
			CBuildFromTableReader bxls = new CBuildFromTableReader(
					xlsUnspecified, "age,foo");
			fail("IOException not genereated");
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}

	}

	@Test
	public void testColumnTypes() throws IOException {
		CBuildFromTableReader bxls = new CBuildFromTableReader(xlsUnspecified,
				"id");

		assertArrayEquals(new Class<?>[] { Object.class, Object.class,
				Object.class, Object.class, Object.class },
				bxls.getColumnTypes());
	}

	@Test
	public void testColumnTypesReaderSpecified() throws IOException {
		CBuildFromTableReader bxls = new CBuildFromTableReader(xlsSpecified,
				"xlspeople", new String[] { "id" });

		assertArrayEquals(peopleTypes, bxls.getColumnTypes());
	}

	@Test
	public void testConstruction() throws IOException, CDataGridException {
		CBuildFromTableReader bxls = new CBuildFromTableReader(xlsUnspecified,
				"id");

		CDataCacheContainer con = new CDataCacheContainer(bxls);
	}

	@Test
	public void testContentsOfDoubleField() throws IOException,
			CDataGridException {
		CBuildFromTableReader bxls = new CBuildFromTableReader(xlsSpecified,
				"xlspeople", new String[] { "id" });

		CDataCacheContainer con = new CDataCacheContainer(bxls);

		CDataRowSet cdrs = con.getAll();

		String age1 = Double.valueOf(18.25).toString();
		String age2 = Double.valueOf(28.25).toString();
		String age3 = Double.valueOf(28.25 + (1.0 / 3.0)).toString();
		String age4 = Double.valueOf(17).toString();
		String age5 = Double.valueOf(18.7635120384).toString();

		cdrs.first();
		assertEquals(age1, cdrs.getString("age"));
		cdrs.next();
		assertEquals(age2, cdrs.getString("age"));
		cdrs.next();
		assertEquals(age3, cdrs.getString("age"));
		System.out.println(cdrs.getString("age"));
		cdrs.next();
		assertEquals(age4, cdrs.getString("age"));
		cdrs.next();
		assertEquals(age5, cdrs.getString("age"));
		cdrs.next();

	}

	@Test
	public void testColumnNames() throws IOException {
		CBuildFromTableReader bxls = new CBuildFromTableReader(xlsUnspecified,
				"id");

		assertArrayEquals(header, bxls.getColumnNames());
	}

	@Test
	public void testFileWithMissingValues() throws IOException,
			CDataGridException {
		CBuildFromTableReader bxls = new CBuildFromTableReader(
				xlsMissingValuesUnspecified, "a");

		CDataCacheContainer con = new CDataCacheContainer(bxls);
	}

}
