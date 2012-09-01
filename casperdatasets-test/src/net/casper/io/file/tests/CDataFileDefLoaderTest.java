package net.casper.io.file.tests;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import net.casper.io.file.def.CDataFileDef;
import net.casper.io.file.def.CDataFileDefLoader;

import org.junit.BeforeClass;
import org.junit.Test;
import org.omancode.rmt.cellreader.CellReader;
import org.omancode.rmt.cellreader.CellReaderException;
import org.omancode.rmt.cellreader.CellReaders;
import org.omancode.rmt.cellreader.CharacterReader;

public class CDataFileDefLoaderTest {

	public static final String TEST_DIR = "resources/";
	public static final File adultsFile = new File(TEST_DIR + "json.txt");
	public static final File numVisitsFile = new File(TEST_DIR + "json number of visits.txt");

	public static final CDataFileDef adultsDef =
			new CDataFileDef(
					"Adults", 
					"updated,sex,weight,age,name,height,children",
					new CellReader<?>[] { CellReaders.BOOLEAN,
							CellReaders.CHARACTER, CellReaders.DOUBLE,
							CellReaders.INTEGER, CellReaders.STRING,
							CellReaders.OPTIONAL_DOUBLE,
							CellReaders.OPTIONAL_INTEGER }, "integer");

	public static final CDataFileDef nonDefaultCellReader =
		new CDataFileDef(
				"Contains non default cell reader",
				"bool,char",
				new CellReader<?>[] { CellReaders.BOOLEAN, new CharacterReader()
						}, "bool");

	
	private static CDataFileDefLoader loader;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		loader = new CDataFileDefLoader();
	}

	@Test
	public void testToAndFromJsonString() {
		
		String json = loader.toJsonString(adultsDef);
		
		System.out.println(json);
		
		CDataFileDef builtCDF = loader.fromJsonString(json);
		
 		assertEquals(adultsDef.toString(), builtCDF.toString());
	}
	
	@Test
	public void testFromJsonFile() throws IOException {
		CDataFileDef builtCDF = loader.fromJsonFile(adultsFile);
		
		assertEquals(adultsDef.toString(), builtCDF.toString());
	}
	
	@Test
	public void testLoadFromJsonNumberOfVisitsFile() throws IOException {
		CDataFileDef builtCDF = loader.fromJsonFile(numVisitsFile);
		
		System.out.println(builtCDF);
		
	}

	@Test(expected=CellReaderException.class)
	public void testNonDefaultCellReader() {
		String json = loader.toJsonString(nonDefaultCellReader);
	}
	

}
