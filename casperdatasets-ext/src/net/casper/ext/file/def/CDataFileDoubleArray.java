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
 * Provides a double array from the columns of a {@link CDataFile}.
 * 
 * @author Oliver Mannion
 * @version $Revision: 193 $
 */
public class CDataFileDoubleArray implements CDataFile {

	private final CDataFile cDataFile;

	private final String sourceColumn;

	private double[] array = null;

	/**
	 * From base components, construct a casper dataset file definition that
	 * returns a double array.
	 * 
	 * @param name
	 *            casper container name.
	 * @param sourceColumn
	 *            The dataset column of {@code cDataFile} that contains doubles.
	 */
	public CDataFileDoubleArray(String name, String sourceColumn) {
		this(new CDataFileDef(name, sourceColumn,
				new CellReader<?>[] { CellReaders.DOUBLE }, null),
				sourceColumn);
	}

	/**
	 * From a {@link CDataFile} construct a casper dataset file definition that
	 * returns a double array.
	 * 
	 * @param cDataFile
	 *            dataset file definition
	 * @param sourceColumn
	 *            The dataset column of {@code cDataFile} that contains doubles.
	 */
	public CDataFileDoubleArray(CDataFile cDataFile, String sourceColumn) {
		this.cDataFile = cDataFile;
		this.sourceColumn = sourceColumn;
	}

	/**
	 * Return the array of doubles loaded from the column.
	 * 
	 * @return double array
	 */
	public double[] getDoubleArray() {
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
			array = CasperUtil.loadDoubleArray(source, sourceColumn);
		} catch (CDataGridException e) {
			throw new IOException("Problem loading double array from "
					+ file.getCanonicalPath(), e);
		}

		return source;
	}

}
