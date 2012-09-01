package net.casper.ext.file.def;

import java.io.File;
import java.io.IOException;

import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.ext.CasperUtil;
import net.casper.io.file.def.CDataFile;
import net.casper.io.file.def.CDataFileDef;

import org.omancode.rmt.cellreader.CellReader;
import org.omancode.rmt.cellreader.CellReaders;

/**
 * Provides a int array from the columns of a {@link CDataFile}.
 * 
 * @author Oliver Mannion
 * @version $Revision: 193 $
 */
public class CDataFileIntArray implements CDataFile {

	private final CDataFile cDataFile;

	private final String sourceColumn;

	private int[] array = null;

	/**
	 * From base components, construct a casper dataset file definition that
	 * returns a int array.
	 * 
	 * @param name
	 *            casper container name.
	 * @param sourceColumn
	 *            The dataset column of {@code cDataFile} that contains ints.
	 */
	public CDataFileIntArray(String name, String sourceColumn) {
		this(new CDataFileDef(name, sourceColumn,
				new CellReader<?>[] { CellReaders.INTEGER }, null),
				sourceColumn);
	}

	/**
	 * From a {@link CDataFile} construct a casper dataset file definition that
	 * returns a int array.
	 * 
	 * @param cDataFile
	 *            dataset file definition
	 * @param sourceColumn
	 *            The dataset column of {@code cDataFile} that contains ints.
	 */
	public CDataFileIntArray(CDataFile cDataFile, String sourceColumn) {
		this.cDataFile = cDataFile;
		this.sourceColumn = sourceColumn;
	}

	/**
	 * Return the array of ints loaded from the column.
	 * 
	 * @return int array
	 */
	public int[] getIntArray() {
		if (array == null) {
			throw new IllegalStateException("Array not been loaded. "
					+ "Has loadDataset(file) been called?");
		}

		return array;
	}

	@Override
	public CDataCacheContainer getContainer() {
		return cDataFile.getContainer();
	}

	@Override
	public String getName() {
		return cDataFile.getName();
	}

	@Override
	public CDataCacheContainer loadDataset(File file) throws IOException {
		CDataCacheContainer source = cDataFile.loadDataset(file);

		try {
			array = CasperUtil.loadIntArray(source, sourceColumn);
		} catch (CDataGridException e) {
			throw new IOException("Problem loading int array from "
					+ file.getCanonicalPath(), e);
		}

		return source;
	}

}
