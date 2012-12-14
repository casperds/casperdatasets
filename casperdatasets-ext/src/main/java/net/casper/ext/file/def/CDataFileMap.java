package net.casper.ext.file.def;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CDataRowSet;
import net.casper.io.file.def.CDataFile;
import net.casper.io.file.def.CDataFileDef;

import org.omancode.rmt.cellreader.CellReader;

/**
 * Provides a map from a key and value column of a {@link CDataFile}.
 * 
 * @author Oliver Mannion
 * @version $Revision: 193 $
 * @param <K>
 *            map's key type
 * @param <V>
 *            map's value type
 */
public class CDataFileMap<K, V> implements CDataFile {

	private final CDataFile cDataFile;

	private final String keyColumn;

	private final String valueColumn;

	private final Map<K, V> map;

	private boolean mapLoaded = false;

	/**
	 * From base components, construct a casper dataset file definition that
	 * returns a map.
	 * 
	 * @param name
	 *            casper container name.
	 * @param keyColumnName
	 *            The dataset column that specifies the keys for the map.
	 * @param keyColumnReader
	 *            The {@link CellReader} for the key column
	 * @param valueColumnName
	 *            The dataset column that specifies the values for the map.
	 * @param valueColumnReader
	 *            The {@link CellReader} for the value column
	 */
	public CDataFileMap(String name, String keyColumnName,
			CellReader<K> keyColumnReader, String valueColumnName,
			CellReader<V> valueColumnReader) {
		this(new CDataFileDef(name, keyColumnName + "," + valueColumnName,
				new CellReader<?>[] { keyColumnReader, valueColumnReader },
				keyColumnName), new LinkedHashMap<K, V>(), keyColumnName,
				valueColumnName);
	}

	/**
	 * From an existing {@link CDataFile} construct a casper dataset file
	 * definition that returns a map.
	 * 
	 * @param cDataFile
	 *            dataset file definition
	 * @param map
	 *            The map that is filled from the dataset.
	 * @param mapKeyColumn
	 *            The dataset column that specifies the keys for the map.
	 * @param mapValueColumn
	 *            The dataset column that specifies the values for the map.
	 */
	public CDataFileMap(CDataFile cDataFile, Map<K, V> map,
			String mapKeyColumn, String mapValueColumn) {
		this.cDataFile = cDataFile;
		this.map = map;
		this.keyColumn = mapKeyColumn;
		this.valueColumn = mapValueColumn;
	}

	/**
	 * Return the array of doubles loaded from the column.
	 * 
	 * @return double array
	 */
	public Map<K, V> getMap() {
		if (!mapLoaded) {
			throw new IllegalStateException("Map not been loaded. "
					+ "Has loadDataset(file) been called?");
		}

		return map;
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
			CDataRowSet rowset = source.getAll();

			while (rowset.next()) {
				@SuppressWarnings("unchecked")
				K key = (K) rowset.getObject(keyColumn);
				@SuppressWarnings("unchecked")
				V value = (V) rowset.getObject(valueColumn);

				map.put(key, value);
			}

			mapLoaded = true;

		} catch (CDataGridException e) {
			throw new IOException("Problem loading map from "
					+ file.getCanonicalPath(), e);
		}

		return source;
	}

}
