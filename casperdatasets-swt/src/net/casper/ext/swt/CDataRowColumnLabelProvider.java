package net.casper.ext.swt;

import net.casper.data.model.CDataRow;

import org.eclipse.jface.viewers.ColumnLabelProvider;

/**
 * Provides column labels for specific column from a {@link CDataRow}.
 * 
 * @author Oliver Mannion
 * @version $Revision: 225 $
 */
public class CDataRowColumnLabelProvider extends ColumnLabelProvider {

	private final int colIndex;

	/**
	 * Create provider that supplies text for the specified column index
	 * (0-based).
	 * 
	 * @param colIndex
	 *            column index
	 */
	public CDataRowColumnLabelProvider(int colIndex) {
		this.colIndex = colIndex;
	}

	@Override
	public String getText(Object element) {
		CDataRow row = (CDataRow) element;
		return row.getRawData()[colIndex].toString();
	}

}
