package net.casper.ext.swt;

import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CRowMetaData;

import org.eclipse.jface.viewers.ArrayContentProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.TableViewerColumn;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.TableColumn;

/**
 * Casper utility class for SWT functions.
 * 
 * @author Oliver Mannion
 * @version $Revision: 225 $
 */
public final class CSWTUtil {

	private CSWTUtil() {

	}

	/**
	 * Load up the columns and data of a {@link TableViewer} from the container.
	 * Columns default to a width of 100, resizable and moveable.
	 * 
	 * @param viewer
	 *            table viewer
	 * @param container
	 *            container
	 * @return the same table viewer
	 * @throws CDataGridException
	 *             if problem reading container
	 */
	public static TableViewer setViewerData(TableViewer viewer,
			CDataCacheContainer container) throws CDataGridException {

		CRowMetaData crmd = container.getMetaDefinition();

		for (int i = 0; i < crmd.getColumnCount(); i++) {

			TableViewerColumn viewerColumn =
					new TableViewerColumn(viewer, SWT.NONE);
			TableColumn column = viewerColumn.getColumn();
			column.setText(crmd.getColumnName(i + 1));
			column.setWidth(100);
			column.setResizable(true);
			column.setMoveable(true);
			viewerColumn.setLabelProvider(new CDataRowColumnLabelProvider(i));
		}

		// set the content provider
		viewer.setContentProvider(new ArrayContentProvider());

		// set the content for the viewer, setInput will call getElements in the
		// contentProvider
		viewer.setInput(container.getAll().getAllRows());

		return viewer;
	}

}
