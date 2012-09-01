package net.casper.data.model.tests;

import net.casper.data.model.CDataComparator;
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CDataRow;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CDataComparatorTest {

	@Test
	public void testIssue4() throws CDataGridException {
		Object[] row1 = {Integer.valueOf(1), Boolean.FALSE, Integer.valueOf(2)};
		Object[] row2 = {Integer.valueOf(1), Boolean.FALSE, Integer.valueOf(1)};
		
		int[] columnIndices  = {0, 1, 2};
		Class[] columnTypes = {Integer.class, Boolean.class, Integer.class};
		
		CDataRow cRow1 = new CDataRow(row1);
		CDataRow cRow2 = new CDataRow(row2);
		
		CDataComparator comparator = new CDataComparator(columnIndices, columnTypes);
		
		assertEquals(1, comparator.compare(cRow1, cRow2));
		assertEquals(-1, comparator.compare(cRow2, cRow1));
		assertEquals(0, comparator.compare(cRow2, cRow2));
		assertEquals(0, comparator.compare(cRow1, cRow1));
	}

}
