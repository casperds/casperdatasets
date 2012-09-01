package net.casper.ext.tests;

import java.io.File;
import java.io.IOException;

import net.casper.data.model.CBuilder;
import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CDataRowSet;
import net.casper.ext.narrow.CBuildNarrowedFile;

import org.junit.Test;

public class CBuildNarrowedFileTest {

	public static final String TEST_DIR = "resources/";

	private File patientsXLS = new File(TEST_DIR + "patients.xls");

	private File patientsCSV = new File(TEST_DIR + "patients.csv");

	@Test
	public void testCBuildNarrowedFromXLS() throws IOException,
			CDataGridException {
		CBuilder builder = new CBuildNarrowedFile(patientsXLS, "refnum,crefnum");
		CDataCacheContainer cdcc = new CDataCacheContainer(builder);
		CDataRowSet cdrs = cdcc.getAll();
		System.out.println(cdrs);
	}

	@Test
	public void testCBuildNarrowedFromXLSNoPKConvertMissing()
			throws IOException, CDataGridException {
		CBuilder builder = new CBuildNarrowedFile(patientsXLS)
				.setConvertMissing(true);
		CDataCacheContainer container = new CDataCacheContainer(builder);
		CDataRowSet cdrs = container.getAll();
		System.out.println(cdrs);
	}

	@Test
	public void testCBuildNarrowedFromCSV() throws IOException,
			CDataGridException {
		// load base file into casper container
		CBuilder builder = new CBuildNarrowedFile(patientsCSV, "refnum,crefnum");
		CDataCacheContainer cdcc = new CDataCacheContainer(builder);
		CDataRowSet cdrs = cdcc.getAll();
		System.out.println(cdrs);

	}

	@Test
	public void testCBuildNarrowedFromCSVNoPKConvertMissing()
			throws IOException, CDataGridException {
		// Load CSV file, using the headers found in the file,
		// and converting each column to the narrowest possible
		// data type. Missing integers and doubles will be 
		// returned as CellReaders.MISSING_VALUE_INTEGER
		// or CellReaders.MISSING_VALUE_DOUBLE
		CBuilder builder = new CBuildNarrowedFile(patientsCSV)
				.setConvertMissing(true);
		CDataCacheContainer container = new CDataCacheContainer(builder);
		CDataRowSet cdrs = container.getAll();
		System.out.println(cdrs);

	}

}
