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
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

@SuppressWarnings("unchecked")
public class SOperatorUtil {

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
		
		if (TestSQLTypes.mode) {
			//Polymorphism method
			srootOp.setColumnMap();
		}
		
		return srootOp;
	}

	/**
	 * Generate all output column info from the root SOperator
	 * @param op
	 */
	public static void generateRootInfo(SOperator rootSop) {
		
		if (rootSop.parents.size() > 0) {
			//generate parents info first
			for (SOperator parent: rootSop.parents) {
				if (parent.columnRootMap == null) {
					generateRootInfo(parent);
				}
			}
			
			//generate node info
			Map<SColumn, SBaseColumn> rootMap = new HashMap<SColumn, SBaseColumn>();
			for (SColumn col: rootSop.columns) {
				boolean found = false;
				for (SOperator parent: rootSop.parents) {
					//transitivity
					//System.out.println("Matching: " + col + " " + parent.columnRootMap.keySet());
					if (parent.columnRootMap.containsKey(rootSop.columnMap.get(col))) {
						rootMap.put(col, parent.columnRootMap.get(rootSop.columnMap.get(col)));
						found = true;
						break;
					}
				}
				if (!found) {
					if (rootSop.op instanceof GroupByOperator) {
						rootMap.put(col, null);
					}
					else if (rootSop.op instanceof ReduceSinkOperator) {
						rootMap.put(col, null);
					}
					else {
						System.out.println("Fatal Error: node not found in parent in generateRootInfo!" + rootSop.op.getClass());
					}
				}
			}
			rootSop.columnRootMap = rootMap;
		}
		else {
			//It must be TableScanOperator
			if (rootSop instanceof STableScanOperator) {
				rootSop.columnRootMap = convertColumnMapToRootMap(rootSop.columnMap, (STableScanOperator) rootSop);
			} else {
				System.out.println("Fatal Error: " + "Parent Size 0 but not TableScanOperator!");
			}
		}
	}
	
	private static Map<SColumn, SBaseColumn> convertColumnMapToRootMap(Map<SColumn, SColumn> columnMap, STableScanOperator sop) {
		Map<SColumn, SBaseColumn> rootMap = new HashMap<SColumn, SBaseColumn>();
		
		for (SColumn scol: columnMap.keySet()) {
			rootMap.put(scol, new SBaseColumn(scol, sop));
		}
		
		return rootMap;
	}
}