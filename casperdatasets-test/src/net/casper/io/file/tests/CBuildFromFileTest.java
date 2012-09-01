package net.casper.io.file.tests;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import net.casper.data.model.CBuilder;
import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CDataRowSet;
import net.casper.io.file.def.CDataFile;
import net.casper.io.file.def.CDataFileDef;
import net.casper.io.file.def.CDataFileDefLoader;
import net.casper.io.file.in.CBuildFromFile;

import org.junit.Test;
import org.omancode.rmt.cellreader.CellReader;
import org.omancode.rmt.cellreader.CellReaders;

public class CBuildFromFileTest {

	public static final String TEST_DIR = "resources/";

	File patientsXLS = new File(TEST_DIR + "patients.xls");
	File patientsCSV = new File(TEST_DIR + "patients.csv");

	private String[] columnNames = new String[] { "crefnum", "sex", "refnum",
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
	public void testCBuildFileUnspecifiedXLS() throws IOException,
			CDataGridException {
		// load base file into casper container
		CBuilder builder = new CBuildFromFile(patientsXLS, "refnum,crefnum");
		CDataCacheContainer cdcc = new CDataCacheContainer(builder);
		cdcc.getAll();
		assertEquals(4, cdcc.size());
	}

	@Test
	public void testCBuildFileUnspecifiedXLSNoPK() throws IOException,
			CDataGridException {
		// load base file into casper container
		CBuilder builder = new CBuildFromFile(patientsXLS, null);
		CDataCacheContainer cdcc = new CDataCacheContainer(builder);
		CDataRowSet cdrs = cdcc.getAll();
		System.out.println(cdrs);
		assertEquals(4, cdcc.size());
	}

	@Test
	public void testCBuildFileUnspecifiedCSV() throws IOException,
			CDataGridException {
		// load base file into casper container
		CBuilder builder = new CBuildFromFile(patientsCSV, "refnum,crefnum");
		CDataCacheContainer cdcc = new CDataCacheContainer(builder);
		cdcc.getAll();
		assertEquals(4, cdcc.size());
	}

	@Test
	public void testCBuildFileSpecifiedCSV() throws IOException,
			CDataGridException {
		// load base file into casper container
		CBuilder builder = new CBuildFromFile(patientsCSV, "patientsCSV",
				columnNames, columnOptionalReaders, PKs);
		CDataCacheContainer cdcc = new CDataCacheContainer(builder);
		cdcc.getAll();
		assertEquals(4, cdcc.size());
	}

	@Test
	public void testCBuildFileSpecifiedXLS() throws IOException,
			CDataGridException {
		// load base file into casper container
		CBuilder builder = new CBuildFromFile(patientsXLS, "patientsXLS",
				columnNames, columnOptionalReaders, PKs);
		CDataCacheContainer cdcc = new CDataCacheContainer(builder);
		cdcc.getAll();
		assertEquals(4, cdcc.size());
	}

	@Test
	public void testCBuildFileSpecifiedXLSViaCDataFileDef() throws IOException,
			CDataGridException {
		// load base file into casper container
		CDataFileDef cdf = new CDataFileDef("cdf patients xls", columnNames,
				columnOptionalReaders, PKs);
		CDataCacheContainer cdcc = cdf.loadDataset(patientsXLS);
		cdcc.getAll();
		assertEquals(4, cdcc.size());
		
		System.out.println(new CDataFileDefLoader().toJsonString(cdf));
	}

}
