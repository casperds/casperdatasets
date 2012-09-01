package net.casper.io.file.def;

import java.io.File;
import java.io.IOException;

import net.casper.data.model.CDataCacheContainer;

/**
 * Represents a dataset loaded from a file.
 * 
 * @author Oliver Mannion
 * @version $Revision: 147 $
 */
public interface CDataFile {

	/**
	 * Sets and loads a dataset from the specified file.
	 * 
	 * @param file
	 *            file to load
	 * @return a dataset container materialised from {@code file}
	 * @throws IOException
	 *             if dataset cannot be loaded from the file.
	 */
	CDataCacheContainer loadDataset(File file) throws IOException;

	/**
	 * Get the loaded container.
	 * 
	 * @return loaded container
	 */
	CDataCacheContainer getContainer();

	/**
	 * Get dataset name.
	 * 
	 * @return dataset name
	 */
	String getName();
}
