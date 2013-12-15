package org.apache.hadoop.hive.ql.cs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

class ColumnNotMappedException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	public ColumnNotMappedException(String s) {
		super(s);
	}
}

/**
 * Current Supported Operator Types:
 * 
 * 0. FileSinkOperator (Ignore)
 * 1. JoinOperator -- join FD
 * 2. SelectOperator
 * 3. FilterOperator -- filter FD
 * 4. ReduceSinkOperator
 * 5. TableScanOperator
 * 6. LimitOperator
 * 7. GroupByOperator
 * 8. ExtractOperator(order by)
 * 9. 
 * 
 * @author victor
 */
@SuppressWarnings("unchecked")
public class SOperator {
	//1. initialized when built
	Operator op;
	List<SColumn> columns; //output columns for this operator
	
	//2. initialized with set call
	List<SOperator> parents;

	//3. only be called if above call success
	Map<SColumn, SAbstractColumn> columnMap; //map from *output* columns to *input* columns

	public SOperator(Operator op) {
		this.op = op;

		//setup Columns
		columns = new ArrayList<SColumn>();

		RowSchema schema = op.getSchema();
		for (ColumnInfo info: schema.getSignature()) {
			SColumn col = new SColumn(info.getInternalName(), info.getTabAlias());
			
			columns.add(col);
		}

		columnMap = new HashMap<SColumn, SAbstractColumn>();
		//initial value
		//for (SColumn col: columns) {
			//columnMap.put(col, col);
		//}
	}

	public void setColumnMap() {
		//intermediate mapping
		Map<String, ExprNodeDesc> columnExprMap = op.getColumnExprMap();
		if (columnExprMap == null || columnExprMap.size() == 0) {
			for (SColumn scol: columns) {
				columnMap.put(scol, scol);
			}
		} else {
			setupMapping(columnExprMap);
		}	
	}
	
	private void setupMapping(Map<String, ExprNodeDesc> columnExprMap) {
		/**
		 * postMap maps from ExprNodeDesc -> SColumn
		 */
		Map<ExprNodeDesc, SColumn> postMap = new HashMap<ExprNodeDesc, SColumn>();
		
		//parents all columns, since table alias only stores in col
		List<SColumn> parentsAllColumns = new ArrayList<SColumn>();
		for (SOperator p: parents) {
			parentsAllColumns.addAll(p.columns);
		}
		
		//only maps ExprNodeColumnDesc type
		for (ExprNodeDesc desc: columnExprMap.values()) {
			if (desc instanceof ExprNodeColumnDesc) {
				for (SColumn parentScol: parentsAllColumns) {
					if (parentScol.equalsToNodeDesc((ExprNodeColumnDesc) desc)) {
						postMap.put(desc, parentScol);
						break;
					}
				}
			} else {
				//do nothing
			}
		}

		//generate final mapping
		for (SColumn col: columns) {
			SAbstractColumn correspond = postMap.get(columnExprMap.get(col.getName()));
			if (correspond == null) {
				correspond = SOperatorFactory.generateSColumnFromDesc(columnExprMap.get(col.getName()), this);
			}
			columnMap.put(col, correspond);
		}
	}

	public SAbstractColumn getRootColumn(SColumn scol) {
		//recursively call its parents until rootColumn or null
		for (SOperator parent: parents) {
			SAbstractColumn key = columnMap.get(scol);
			if (key instanceof SColumn) {
				if (parent.columnMap.containsKey( key )) {
					return parent.getRootColumn( (SColumn) key );
				}
			}
		}

		throw new ColumnNotMappedException(this.getClass() + "   " + columns + "  " + columnMap);
	}

	//bean methods
	
	public void setParents(List<SOperator> parents) {
		this.parents = parents;
	}
	
	public String toString() {
		return op.getClass() + "\n" + columnMap.toString();
	}
	
	public ArrayList<SAbstractColumn> getAllRootColumns() {
		ArrayList<SAbstractColumn> lst = new ArrayList<SAbstractColumn>();
		for (SColumn scol: columns) {
			lst.add(getRootColumn(scol));
		}
		return lst;
	}
	
	public String prettyString() {
		return getAllRootColumns().toString();
	}
}