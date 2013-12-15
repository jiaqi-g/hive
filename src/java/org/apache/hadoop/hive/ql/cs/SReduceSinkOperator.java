package org.apache.hadoop.hive.ql.cs;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;

/**
 * ReduceSinkOperator may have constant ExprNodeDesc
 * 
 * @author victor
 *
 */
public class SReduceSinkOperator extends SOperator {
	
	ReduceSinkDesc desc;
	
	public SReduceSinkOperator(ReduceSinkOperator op) {
		super(op);
		desc = ((ReduceSinkOperator)op).getConf();
	}
	
	private boolean isDerivedBaseColumn(SOperator sop) {
		return false;
	}
	
	@Override
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
		
		return null;
	}
	
	public String prettyString() {
		return super.prettyString() + " Key Cols: " + desc.getKeyCols() + " Value Cols: " + desc.getValueCols()
						+ " Partitioned Cols: " + desc.getPartitionCols();
	}
	
	public String toString() {
		return super.toString();
		//return super.toString() + "Key Cols: " + desc.getKeyCols() + "Value Cols: " + desc.getValueCols()
		//		+ "Partitioned Cols: " + desc.getPartitionCols();
	}
	
}