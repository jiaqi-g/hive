package org.apache.hadoop.hive.ql.cs;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;

public class TestSQLTypes {

	public static boolean mode = true;
	public static String factTableName = "lineitem"; //all to lower cases to test
	public static HashMap<String, HashSet<String>> tableToPrimaryKeyMap;
	public static String path = "/home/victor/primarykey.txt";

	public boolean r1 = false;
	public boolean r2 = false;
	public boolean r3 = false;
	public boolean r4 = false;

	public SOperator groupByOpStore;

	static {
		/*
		Scanner sc = null;
		try {
			sc = new Scanner(new File(path));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		tableToPrimaryKeyMap = new HashMap<String, HashSet<String>>();

		try {
			String tmp = null;

			while (sc.hasNextLine()) {
				tmp = sc.nextLine();
				String[] arrs = tmp.split(":");

				String name = arrs[0];
				HashSet<String> set = new HashSet<String>();
				String[] cols = arrs[1].split(",");
				for (String col: cols) {
					set.add(col);
				}

				tableToPrimaryKeyMap.put(name, set);
			}
		}
		catch (Exception e) {
			throw new RuntimeException("Invalid Input file!");
		}
		 */
	}

	public TestSQLTypes() {
	}

	private String getTableOriginalName(STableScanOperator sOperator) {
		String tableName = sOperator.tableName;
		if (tableName != null) {
			tableName = tableName.toLowerCase();
		} else {
			System.out.println("Fatal Error: STableScanOperator does not have original table name!");
		}
		return tableName;
	}

	public int test(SOperator sOperator) {

		//if (!hasGroupBy(sOperator)) return 4;

		if (hasUnion(sOperator)) return 0;

		if (isType0(sOperator)) return 0;
		if (isType1(sOperator)) return 1;
		if (isType2(sOperator)) return 2;
		if (isType3(sOperator)) return 3;
		//if (isType4(sOperator, new HashSet<FD>())) return 4;

		return 4;
	}

	private Set<String> transform(HashSet<String> primaryKeyBasics, STableScanOperator tableScanStore) {

		HashSet<String> set = new HashSet<String>();
		for (String key: primaryKeyBasics) {
			set.add(tableScanStore.id + key);
		}
		return set;
	}

	private void extractFDs(ExprNodeDesc rootExpr, HashSet<FD> fdSet) {
		if (rootExpr instanceof ExprNodeGenericFuncDesc) {
			ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc)rootExpr;
			GenericUDF udf = funcDesc.getGenericUDF();

			List<ExprNodeDesc> childExprs = funcDesc.getChildExprs();
			if (udf instanceof GenericUDFOPAnd) {

				for (ExprNodeDesc d: childExprs) {
					extractFDs(d, fdSet);
				}

			} else if (udf instanceof GenericUDFOPEqual) {

				ExprNodeDesc leftChild = childExprs.get(0);
				ExprNodeDesc rightChild = childExprs.get(1);
				if (leftChild instanceof ExprNodeColumnDesc || leftChild instanceof ExprNodeConstantDesc) {
					if (rightChild instanceof ExprNodeColumnDesc || rightChild instanceof ExprNodeConstantDesc) {
						//fdSet.add(new FD(, ));
					}
				}
			}
		}
		
	}
	
	private STableScanOperator checkType4(SOperator sOperator, HashSet<FD> fdHashSet) {

		//only one will satisfy
		STableScanOperator tableScanStore = null;

		for (SOperator parent : sOperator.parents) {
			tableScanStore = checkType4(parent, fdHashSet);
			if (tableScanStore != null) {
				break;
			}
		}


		if (sOperator.op instanceof TableScanOperator) {
			//store FD

			if (!tableScanStore.tableName.toLowerCase().equals(factTableName)) {
				return null;
			}

			Set<String> primaryKeys = transform(tableToPrimaryKeyMap.get(getTableOriginalName(tableScanStore)), tableScanStore);
			Set<String> allColumns = tableScanStore.getAllColumnsIds();

			fdHashSet.add(new FD(primaryKeys, allColumns));

			return tableScanStore;

		} else if (sOperator.op instanceof FilterOperator) {
			//store FD
			HashSet<FD> fds = new HashSet<FD>();
			extractFDs(((SFilterOperator) sOperator).expr, fds);

		} else if (sOperator.op instanceof JoinOperator) {
			//store FD
			//return 
		} else if (sOperator.op instanceof SelectOperator) {
			//judge
			Collection<SBaseColumn> outputColumns = sOperator.getColumnRootMap().values();
			Set<String> det = new HashSet<String>();
			for (SBaseColumn bcol: outputColumns) {
				if (bcol != null) {
					det.add(bcol.getId());
				}
			}

			Collection<SBaseColumn> inputColumns = sOperator.parents.get(0).getColumnRootMap().values();
			Set<String> inputs = new HashSet<String>();
			for (SBaseColumn bcol: inputColumns) {
				if (bcol != null) {
					inputs.add(bcol.getId());
				}
			}

			/**
			 * potentially wrong
			 */
			if (FD.judge(FD.infer(det, fdHashSet), inputs)) {
				r4 = true;
			}
		} else if (sOperator.op instanceof GroupByOperator) {
			//judge
		}

		return tableScanStore;
	}

	private boolean checkFactTable(SOperator sOperator) {

		List<Boolean> results = new ArrayList<Boolean>();

		for (SOperator parent : sOperator.parents) {
			results.add(checkFactTable(parent));
		}

		if (sOperator.op instanceof TableScanOperator) {
			String tableOriginalName = getTableOriginalName((STableScanOperator)sOperator);

			if (tableOriginalName.equals(factTableName)) {
				return true;
			} else {
				return false;
			}
		} else if (sOperator.op instanceof JoinOperator) {
			boolean t = false;
			//System.out.println(results.size());
			for (Boolean b: results) {
				//System.out.println("====t  " + t);
				if (t && b) {
					r3 = true;
				}
				t = t || b;
			}
		} else if (sOperator.op instanceof GroupByOperator) {
			return false;
		}

		boolean re = false;
		for (Boolean r: results) {
			re = re || r;
		}

		return re;
	}

	private boolean isType3(SOperator sOperator) {
		checkFactTable(sOperator);
		return r3;
	}

	private boolean hasUnion(SOperator sOperator) {
		boolean e = false;

		for (SOperator parent : sOperator.parents) {
			e = e || hasUnion(parent);
		}

		if (sOperator.op instanceof UnionOperator) {
			e = true;
		}

		return e;
	}

	private boolean hasGroupBy(SOperator sOperator) {
		boolean e = false;

		if (sOperator.parents.size() > 0) {
			for (SOperator parent : sOperator.parents) {
				e = e || exist(parent);
			}
		}

		if (sOperator instanceof SGroupByOperator) {
			e = true;
		}

		return e;
	}

	private boolean isType0(SOperator sOperator) {
		boolean r = false;

		if (sOperator.parents.size() > 0) {
			for (SOperator parent : sOperator.parents) {
				r = isType0(parent);
				if (r == true) {
					//System.out.println("Here 1");
					return r;
				}
			}
		}

		Operator rootOp = sOperator.op;
		if (rootOp instanceof GroupByOperator) {
			GroupByOperator op = (GroupByOperator)sOperator.op;

			ArrayList<AggregationDesc> lst = op.getConf().getAggregators();
			for (AggregationDesc desc: lst) {
				ArrayList<ExprNodeDesc> exprDesc = desc.getParameters();

				for (ExprNodeDesc d : exprDesc) {
					r = search(sOperator, d.getExprString());
					if (r == true) {
						//System.out.println("Here 2");
						return r;
					}
				}
			}

			ArrayList<ExprNodeDesc> keys = op.getConf().getKeys();
			for (ExprNodeDesc key: keys) {
				r = search(sOperator, key.getExprString());
				if (r == true) {
					//System.out.println("Here 3");
					return r;
				}
			}
		}

		return r;
	}

	private boolean search(SOperator sop, String expr) {
		SOperator parent = sop.parents.get(0);

		Set<SColumn> set = parent.getColumnRootMap().keySet();
		for (SColumn s: set) {
			if (s.name.equals(expr)) {
				if (parent.getColumnRootMap().get(s) == null) {
					return true;
				}
			}
		}

		return false;
	}

	private void testAggregate(String name) {
		if (name.equals("sum") || name.equals("avg") || name.equals("count")) {}
		else {
			r1 = true;
		}
	}

	private void checkType1(SOperator sOperator) {
		if (sOperator.parents.size() > 0) {
			for (SOperator parent : sOperator.parents) {
				checkType1(parent);
			}
		}

		if (sOperator.op instanceof GroupByOperator) {
			GroupByOperator op = (GroupByOperator)sOperator.op;
			ArrayList<AggregationDesc> lst = op.getConf().getAggregators();

			for (AggregationDesc desc: lst) {
				testAggregate(desc.getGenericUDAFName());
			}
		}
	}

	private boolean isType1(SOperator sOperator) {
		checkType1(sOperator);
		if (r1) {
			return true;
		} else {
			return false;
		}
	}

	private boolean isType2(SOperator sOperator) {
		if (!hasGroupBy(sOperator)) return false;

		r2 = true;
		exist(sOperator);
		return r2;
	}

	private boolean exist(SOperator sOperator) {

		boolean e = false;

		if (sOperator.parents.size() > 0) {
			for (SOperator parent : sOperator.parents) {
				e = e || exist(parent);
			}
		}

		if (e) {
			Operator rootOp = sOperator.op;

			if (rootOp instanceof FilterOperator ||
					rootOp instanceof CommonJoinOperator ||
					rootOp instanceof GroupByOperator) {
				r2 = false;
			} else if (rootOp instanceof SelectOperator) {
				try {
					if (sOperator.getColumnRootMap().values().containsAll(
							groupByOpStore.getColumnRootMap().values())) {}
					else {
						r2 = false;
					}
				}
				catch (Exception ex) {}
			}

			//LimitOperator //limit
			//ExtractOperator) //order by
		}

		if (sOperator instanceof SGroupByOperator && !((SGroupByOperator)sOperator).isDistinct) {
			groupByOpStore = sOperator;
			return true;
		}
		else {
			return e;
		}
	}

}
