package org.apache.hadoop.hive.ql.cs;

import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

/**
 * Order By Operator
 * @author victor
 *
 */
public class SExtractOperator extends SOperator {

	public SExtractOperator(ExtractOperator op) {
		super(op);
	}

	@Override
	public void setColumnMap() {
		columnMap = new HashMap<SColumn, SColumn>();
		if (parents.size() == 0) {
			System.out.println("Fatal Error: SExtractOperator Parents do not exist!");
		}
		else {
			for (SColumn scol: parents.get(0).columns) {
				columnMap.put(scol, scol);
			}
		}
	}

	@Override
	public String prettyString() {
		if (columnRootMap != null) {
			return columnRootMap.values().toString() + " Order By";
		}
		
		return "null";
	}
}