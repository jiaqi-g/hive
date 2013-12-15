package org.apache.hadoop.hive.ql.cs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

@SuppressWarnings("unchecked")
public class SOperatorFactory {
	
	/**
	 * Generate the corresponding SOperator Tree given Operator Tree
	 * @param rootOp
	 */
	@SuppressWarnings("rawtypes")
	public static SOperator generateSOperatorTree(Operator rootOp,
			LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx) {
		
		if (rootOp == null) return null;
		//make sure there is only one FileSinkOperator
		//ignore
		while (rootOp instanceof FileSinkOperator) {
			rootOp = (Operator) rootOp.getParentOperators().get(0);
		}
		
		//create node
		SOperator srootOp = null;
		if (rootOp instanceof FilterOperator) {
			srootOp = new SFilterOperator((FilterOperator) rootOp);
		}
		else if (rootOp instanceof LimitOperator) {
			srootOp = new SLimitOperator((LimitOperator) rootOp);
		}
		else if (rootOp instanceof GroupByOperator) {
			srootOp = new SGroupByOperator((GroupByOperator) rootOp);
		}
		else if (rootOp instanceof ExtractOperator) {
			srootOp = new SExtractOperator((ExtractOperator) rootOp);
		}
		else if (rootOp instanceof ReduceSinkOperator) {
			srootOp = new SReduceSinkOperator((ReduceSinkOperator) rootOp);
		}
		else if (rootOp instanceof TableScanOperator) {
			srootOp = new STableScanOperator((TableScanOperator) rootOp);
			((STableScanOperator)(srootOp)).setTableName(
					opParseCtx.get(rootOp).getRowResolver().tableOriginalName);
		}
		else {
			srootOp = new SOperator(rootOp);
		}
		
		//setup parents
		List<Operator> lst = rootOp.getParentOperators();
		List<SOperator> parents = new ArrayList<SOperator>();

		if (lst != null) {
			for (Operator l: lst) {
				parents.add(generateSOperatorTree(l, opParseCtx));
			}
		}
		srootOp.setParents(parents);
		
		//Polymorphism method
		srootOp.setColumnMap();
		
		return srootOp;
	}

	public static SDerivedColumn generateSColumnFromDesc(ExprNodeDesc nodeDesc, SOperator sop) {
		if (nodeDesc instanceof ExprNodeConstantDesc) {
			return new SConstant((ExprNodeConstantDesc) nodeDesc);
		} else if (nodeDesc instanceof ExprNodeGenericFuncDesc) {
			return new SFunction((ExprNodeGenericFuncDesc) nodeDesc, sop);
		} else if (nodeDesc instanceof ExprNodeColumnDesc) {
			return new SColumn((ExprNodeColumnDesc) nodeDesc);
		} else {
			System.out.println("Fatal Error: Unsuppored ExprNode Type!" + nodeDesc.getClass());
			return null;
		}
	}
}