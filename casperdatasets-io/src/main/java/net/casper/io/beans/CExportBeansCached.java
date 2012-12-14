package net.casper.io.beans;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import net.casper.data.model.CExporter;

import org.apache.commons.lang.WordUtils;
import org.supercsv.util.MethodCache;

/**
 * Exports a casper dataset to a collection of java beans using a
 * {@link MethodCache} for reflection. The first time it introspects the
 * instance's class, while subsequent method lookups are cached. This is faster
 * than {@link CExportBeans} for larger collections.
 * 
 * @author Oliver Mannion
 * @version $Revision: 147 $
 * @param <E>
 *            type of bean to be created from casper data.
 * 
 */
public class CExportBeansCached<E> implements CExporter {

	/**
	 * Collection of beans created from dataset rows.
	 */
	private final Collection<E> beans = new ArrayList<E>();

	/**
	 * Class of the beans.
	 */
	private final Class<E> beanClass;

	/**
	 * Names of the properties on beans. Same as column names.
	 */
	private List<String> propertyNames = new LinkedList<String>();

	/**
	 * Types of the properties on beans. Same as column types.
	 */
	private Class<?>[] propertyTypes;

	/**
	 * Construct a casper exporter that creates beans of class
	 * <code>beanClass</code>. The bean class must provide a empty constructor
	 * and property setters with the same name as each of the columns in the
	 * exporter dataset.
	 * 
	 * @param beanClass
	 *            class of beans that will be created.
	 */
	public CExportBeansCached(Class<E> beanClass) {
		this.beanClass = beanClass;
	}

	@Override
	public void setName(String cacheName) throws IOException {
		// nothing to do
	}

	@Override
	public void setColumnNames(String[] columnNames) throws IOException {

		// decapitalise the first letter of the column name
		// to match the appropriate bean property can be found
		for (String columnName : columnNames) {
			propertyNames.add(WordUtils.uncapitalize(columnName));
		}
	}

	@Override
	public void setColumnTypes(Class[] columnTypes) throws IOException {
		propertyTypes = columnTypes;
	}

	@Override
	public void setPrimaryKeyColumns(String[] primaryKeyColumns)
			throws IOException {
		// nothing to do
	}

	@Override
	public void open() throws IOException {
		// nothing to do
	}

	// MethodCache. First time it introspects the instance's class, while
	// subsequent method lookups are super fast.
	private final MethodCache cache = new MethodCache();

	@Override
	public void writeRow(Object[] row) throws IOException {
		try {

			// create new bean
			E resultBean = beanClass.newInstance();

			int col = 0;

			// fill bean's properties
			for (String propName : propertyNames) {
				cache.getSetMethod(resultBean, propName, propertyTypes[col])
						.invoke(resultBean, row[col++]);
			}

			// add bean to collection
			beans.add(resultBean);

		} catch (InstantiationException e) {
			throw new IOException("InstantiationException: make sure "
					+ beanClass.getCanonicalName()
					+ " has a nullary constructor", e);
		} catch (IllegalAccessException e) {
			throw new IOException("IllegalAccessException: make sure "
					+ "nullary constructor of " + beanClass.getCanonicalName()
					+ " is accessible", e);
		} catch (InvocationTargetException e) {
			throw new IOException(e);
		}
	}

	@Override
	public Object close() {
		return beans;
	}

	/**
	 * Get exported collection of beans.
	 * 
	 * @return bean collection.
	 */
	public Collection<E> getBeans() {
		return beans;
	}

}