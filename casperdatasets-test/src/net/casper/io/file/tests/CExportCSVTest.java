package net.casper.io.file.tests;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import net.casper.data.model.CBuilder;
import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CDataRowSet;
import net.casper.io.file.in.CBuildFromFile;
import net.casper.io.file.out.CExportCSV;

import org.junit.Test;
import org.omancode.rmt.cellreader.CellReader;
import org.omancode.rmt.cellreader.CellReaders;

public class CExportCSVTest {

	public static final String TEST_DIR = "resources/";

	File patientsCSV = new File(TEST_DIR + "patients.csv");
	File patientsCSVExport = new File(TEST_DIR + "exported-patients.csv");
	File patientsCSVExport2 = new File(TEST_DIR + "exported-patients-2.csv");

	private String[] columnNames = new String[] { "refnum", "sex", "crefnum",
			"question", "age", "weight" };
	private String[] PKs = new String[] { "refnum", "crefnum" };

	private CellReader<?>[] columnOptionalReaders = new CellReader<?>[] {
			CellReaders.OPTIONAL_INTEGER, CellReaders.CHARACTER,
			CellReaders.OPTIONAL_INTEGER, CellReaders.OPTIONAL_INTEGER,
			CellReaders.OPTIONAL_INTEGER, CellReaders.OPTIONAL_DOUBLE };

	private CellReader<?>[] columnReaders = new CellReader<?>[] {
			CellReaders.INTEGER, CellReaders.CHARACTER, CellReaders.INTEGER,
			CellReaders.INTEGER, CellReaders.INTEGER, CellReaders.DOUBLE };

	@Test
	public void testExportCSV() throws IOException, CDataGridException {
		// load base file into casper container
		CBuilder builder = new CBuildFromFile(patientsCSV, "patientsCSV",
				columnNames, columnOptionalReaders, PKs);
		CDataCacheContainer container = new CDataCacheContainer(builder);
		assertEquals(4, container.size());
		
		// Export complete dataset to a csv file
		CExportCSV csv = new CExportCSV(patientsCSVExport);
		container.export(csv);
	}

	@Test
	public void testExportCSVSpecifiedColumns() throws IOException, CDataGridException {
		// load base file into casper container
		CBuilder builder = new CBuildFromFile(patientsCSV, "patientsCSV",
				columnNames, columnOptionalReaders, PKs);
		CDataCacheContainer container = new CDataCacheContainer(builder);
		assertEquals(4, container.size());
		
		// Export only specified columns of dataset to a csv file
		CExportCSV csv = new CExportCSV(patientsCSVExport2,"refnum,sex,crefnum,age");
		container.export(csv);
	}

}
