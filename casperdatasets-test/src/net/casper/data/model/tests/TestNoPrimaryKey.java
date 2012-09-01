package net.casper.data.model.tests;


import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;

import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CDataRowSet;
import net.casper.data.model.CRowMetaData;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestNoPrimaryKey {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Test
	public void testCreation() throws CDataGridException {
		// create container with null primary key
		String columnNames = "Letter";
		final Class<?>[] columnTypes =
				new Class[] { Character.class};

		CRowMetaData metaDef =
			new CRowMetaData(columnNames.split(","), columnTypes,
					null);
		
		// NB: we use a LinkedHashMap here so the container is ordered
		// according to insertion order
		CDataCacheContainer container =
				new CDataCacheContainer("Letters", metaDef, new LinkedHashMap());
		
		for (int i = 0; i < 26; i++) {
			container.addSingleRow(new Character[] {Character.valueOf((char)(65 + i))} );
		}

		int i = 0;
		
		CDataRowSet cdrs = container.getAll();
		while (cdrs.next()) {
			assertEquals(Character.valueOf((char)(65 + i++)), cdrs.getChar("Letter"));
		}
	}
	
	@Test
	public void testCreationViaNewInsertionOrdered() throws CDataGridException {
		// create container with null primary key
		String columnNames = "Letter";
		final Class<?>[] columnTypes =
				new Class[] { Character.class};

		CDataCacheContainer container = CDataCacheContainer.newInsertionOrdered("Letters", 
				columnNames, columnTypes);
		
		
		for (int i = 0; i < 26; i++) {
			container.addSingleRow(new Character[] {Character.valueOf((char)(65 + i))} );
		}

		int i = 0;
		
		CDataRowSet cdrs = container.getAll();
		while (cdrs.next()) {
			assertEquals(Character.valueOf((char)(65 + i++)), cdrs.getChar("Letter"));
		}
	}

}
