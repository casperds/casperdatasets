package net.casper.io.file.in;

import java.io.File;
import java.io.IOException;

import net.casper.data.model.CBuilder;

import org.omancode.rmt.cellreader.CellReader;

/**
 * Interface for creating a CBuilder that loads from a file.
 * 
 * @author Oliver Mannion
 * @version $Revision: 147 $
 */
public interface CBuilderFileFactory {

	/**
	 * Create a new instance of a CBuilder that loads a file.
	 * 
	 * @param file
	 *            file to load
	 * @param name
	 *            name of dataset
	 * @param columnNames
	 *            name of the columns expected.
	 * @param cellReaders
	 *            the {@link CellReader} for each
	 * @param primaryKeys
	 *            those keys that can be used as lookup keys in the returned
	 *            casper dataset
	 * @return a casper dataset
	 * @throws IOException
	 *             if error loading file.
	 */
	CBuilder newBuilder(File file, String name, String[] columnNames,
			CellReader<?>[] cellReaders, String[] primaryKeys)
			throws IOException;
}
