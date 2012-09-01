package net.casper.io.joda;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import net.casper.data.model.CExporter;

import org.joda.property.Bean;

/**
 * Exports a casper dataset to a collection of Joda Beans.
 * 
 * @author Oliver Mannion
 * @version $Revision: 95 $
 * @param <E>
 *            type of bean to be created from casper data.
 * 
 */
public class CExportJodaBeans<E extends Bean> implements CExporter {

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
	private String[] propertyNames;

	/**
	 * Types of the properties on beans. Same as column types.
	 */
	private Class<?>[] propertyTypes;

	/**
	 * Construct a casper exporter that creates beans {@code file}.
	 * 
	 * @param beanClass
	 *            class of beans that will be created.
	 * @throws IOException
	 *             if problem creating file.
	 */
	public CExportJodaBeans(Class<E> beanClass) throws IOException {
		this.beanClass = beanClass;
	}

	@Override
	public void setName(String cacheName) throws IOException {
		// nothing to do
	}

	@Override
	public void setColumnNames(String[] columnNames) throws IOException {
		propertyNames = columnNames;
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

	@Override
	public void writeRow(Object[] row) throws IOException {
		try {

			// create new bean
			E bean = beanClass.newInstance();

			// fill bean's properties
			JodaBeanUtil.fillBean(bean, propertyNames, propertyTypes, row);

			// add bean to collection
			beans.add(bean);

		} catch (InstantiationException e) {
			throw new IOException(e);
		} catch (IllegalAccessException e) {
			throw new IOException(e);
		} catch (PropertyException e) {
			throw new IOException(e);
		}

	}

	@Override
	public Object close() {
		// nothing to do
		return null;
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