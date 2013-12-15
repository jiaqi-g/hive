package org.apache.hadoop.hive.ql.cs;

import java.io.PrintStream;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hive.ql.optimizer.ColumnPrunerProcFactory;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.optimizer.JoinReorder;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;

//victor

/**
 * This is a hacking class.
 * Another modification is Driver.java which needs to call compile when call execute
 * 
 * @author victor
 */
public class ExplainTaskHelper {
	PrintStream out;
	ExplainWork work;
	static LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx;
	
	private Set<Operator<? extends OperatorDesc>> visitedOps = new HashSet<Operator<?>>();

	public ExplainTaskHelper(PrintStream out, ExplainWork work) {
		this.out = out;
		this.work = work;
	}

	private boolean isPrintable(Object val) {
		if (val instanceof Boolean || val instanceof String
				|| val instanceof Integer || val instanceof Long || val instanceof Byte
				|| val instanceof Float || val instanceof Double) {
			return true;
		}

		if (val != null && val.getClass().isPrimitive()) {
			return true;
		}

		return false;
	}

	private boolean shouldPrint(Explain exp, Object val) {
		if (exp.displayOnlyOnTrue() && (val instanceof Boolean) & !((Boolean)val)) {
			return false;
		}
		return true;
	}

	private void output(Serializable work) {

		if (work == null) return;

		// Check if work has an explain annotation
		Annotation note = work.getClass().getAnnotation(Explain.class);
		//if (note == null) return;

		if (note instanceof Explain) {
			Explain xpl_note = (Explain) note;
			out.println(xpl_note.displayName());
		}

		out.println(work.getClass());
		// If this is an operator then we need to call the plan generation on conf and then the children
		if (work instanceof Operator) {

			/**
			 * explain only in Driver #execute
			 * optimize(generate alias map) in Driver #compile
			 */
			if (work instanceof MapJoinOperator) {
				//map join tables
				MapJoinOperator c = (MapJoinOperator) work;
				//out.println("Join" + (c.getColumnExprMap()));
				//out.println(c.get);

				//desc not contains useful infos

				MapJoinDesc desc = c.getConf();
				//out.println(desc.getKeyTableDesc());

				//out.println(desc.getKeys()); //0 1
				//out.println(desc.getRetainList());
				out.println(desc.getExprs());
				//out.println(desc.getAliasBucketFileNameMapping()); null
				//out.println(desc.getBigTableBucketNumMapping());  {}
				//out.println(desc.getBigTablePartSpecToFileMapping()); null

				//List<TableDesc> descs = desc.getValueTblDescs();
				//for (TableDesc d : descs) {
				//out.println(d.getJobProperties());
				//}
				//+ " alias map " + ((CommonJoinOperator)work).getPosToAliasMap());
			}

			//conf is the table info
			Operator<? extends OperatorDesc> operator = (Operator<? extends OperatorDesc>) work;
			if (operator.getConf() != null) {
				String appender = " (" + operator.getOperatorId() + ")";
				out.println(appender);
			}

			if (!visitedOps.contains(operator)) {
				visitedOps.add(operator);
				if (operator.getChildOperators() != null) {
					for (Operator<? extends OperatorDesc> op : operator.getChildOperators()) {
						output(op);
					}
				}
			}
			return;
		}

		// We look at all methods that generate values for explain
		Method[] methods = work.getClass().getMethods();

		for (Method m : methods) {
			note = m.getAnnotation(Explain.class);

			if (note instanceof Explain) {
				Explain xpl_note = (Explain) note;

				if (xpl_note.normalExplain()) {

					Object val = null;

					try {
						val = m.invoke(work);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						val = null;
					}


					if (val == null) {
						continue;
					}

					String header = null;
					boolean skipHeader = xpl_note.skipHeader();
					//boolean emptyHeader = false;

					// Try the output as a primitive object
					if (isPrintable(val)) {
						if (out != null && shouldPrint(xpl_note, val)) {
							if (!skipHeader) {
								out.printf("%s ", header);
							}
							out.println(val);
						}
						continue;
					}

					// Try this as a map
					try {
						// Go through the map and print out the stuff
						Map<?, ?> mp = (Map<?, ?>) val;

						if (out != null && !skipHeader && mp != null && !mp.isEmpty()) {
							out.print(header);
						}

						continue;
					}
					catch (ClassCastException ce) {
						// Ignore - all this means is that this is not a map
					}

					// Try this as a list
					try {
						List<?> l = (List<?>) val;

						if (out != null && !skipHeader && l != null && !l.isEmpty()) {
							out.print(header);
						}

						continue;
					}
					catch (ClassCastException ce) {
						// Ignore
					}

					// Finally check if it is serializable
					try {
						Serializable s = (Serializable) val;

						if (!skipHeader && out != null) {
							out.println(header);
						}
						continue;
					}
					catch (ClassCastException ce) {
						// Ignore
					}
				}
			}
		}
	}

	private void outputPlan(Task<? extends Serializable> task) {
		if (task == null) return;

		out.printf("Stage: \n", task.getId());

		//real output
		Serializable work = task.getWork();
		if (work == null) return;

		if (work instanceof FetchWork) {
			out.println("Fetch");
			output(((FetchWork)work).getSource());
		}
		else if (work instanceof MapredLocalWork) {
			out.println("MapredLocalWork");
			//fetch
			try {
				out.println("Fetch Part");
				Collection<FetchWork> fetchWorkCollect =
						((MapredLocalWork)work).getAliasToFetchWork().values();
				for (FetchWork f : fetchWorkCollect) {
					output(f.getSource());
				}
			}
			catch (Exception e) {out.println("Exception 1");}

			//others
			try {
				out.println("Other Parts");
				Collection<Operator<? extends OperatorDesc>> collect =
						((MapredLocalWork)work).getAliasToWork().values();

				for (Operator<? extends OperatorDesc> c : collect) {
					output(c);
				}
			}
			catch (Exception e) {out.println("Exception 2");}
		}
		else if (work instanceof MapredWork) {
			out.println("MapredWork");
			try {
				Collection<Operator<? extends OperatorDesc>> collect =
						((MapredWork)work).getAllOperators();

				for (Operator<? extends OperatorDesc> c : collect) {
					//out.println(1);
					output(c);
					break; //first operator will give out all info s
				}
			}
			catch (Exception e) {out.println("Exception 3");}
		}
		else {
			output(work);
		}

		//-------other cases--------------------
		if (task instanceof ConditionalTask
				&& ((ConditionalTask) task).getListTasks() != null) {
			for (Task<? extends Serializable> con : ((ConditionalTask) task).getListTasks()) {
				outputPlan(con);
			}
		}

		if (task.getChildTasks() != null) {
			for (Task<? extends Serializable> child : task.getChildTasks()) {
				outputPlan(child);
			}
		}
	}

	/**
	 * Deprecated: called from {@link ExplainTask#execute(org.apache.hadoop.hive.ql.DriverContext)}
	 */
	public void execute() {
		//out.println("----------------------");

		//out.println("reborn!");
		//System.out.println("Console output!!!!!!");
		//out.println("Use other options");
		/*
		for (ReadEntity input: work.getInputs()) {
			switch (input.getType()) {
			case TABLE:
				Table table = input.getTable();
				Map<String, String> tableInfo = new HashMap<String, String>();
				tableInfo.put("tablename", table.getCompleteName());
				//tableInfo.put("tabletype", table.getTableType().toString());
			}
		}
		 */

		//ArrayList<Task<? extends Serializable>> tasks = work.getRootTasks();

		//for (Task<? extends Serializable> rootTask : tasks) {
		//outputPlan(rootTask);
		//possible table info store
		//rootTask.getQueryPlan();
		//JoinDesc
		//}

		//out.println("----------------------");
	}

	/**
	 * Deprecated: called from {@link Driver#compile(String, boolean)}
	 */
	public static void compile() {
		//System.out.println("--------------------called from Driver.compile success!!!!!!-------------------");
	}

	/**
	 * Deprecated: called from {@link JoinReorder#transform(org.apache.hadoop.hive.ql.parse.ParseContext)}
	 */
	public static void join(JoinOperator joinOp, ParseContext pactx,  Set<String> bigTables){
		//System.out.println(pactx.getJoinContext().keySet().size());
		//System.out.println(pactx.getParseTree().dump());
		//System.out.println(pactx.getQB().getAliases()); all aliases
		//System.out.println(pactx.getNameToSplitSample()); null
		//System.out.println(pactx.getIdToTableNameMap()); null
		//System.out.println(pactx.getViewAliasToInput()); null
		//System.out.println(joinOp.getPosToAliasMap());
		//System.out.println("names: " + bigTables);
	}

	/**
	 * called from {@link SemanticAnalyzer#analyzeInternal(org.apache.hadoop.hive.ql.parse.ASTNode)}
	 */
	public static void analyze(Operator sinkOp, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx) {
		//System.out.println("SinkOp passed in");
		if (sinkOp == null) return;
		ExplainTaskHelper.opParseCtx = opParseCtx;
		
		try {
			SOperator sop = SOperatorFactory.generateSOperatorTree(sinkOp, opParseCtx);
			printSop(0, sop);
			if (TestSQLTypes.mode) {
				//System.out.println("!!!!!!TYPE: " +  new TestSQLTypes().test(sop));
			}
			//System.out.println(TestSQLTypes.tableToPrimaryKeyMap);
		}
		catch (Exception e) {
			System.out.println("----Error in generateSOperatorTree ---");
			e.printStackTrace();
		}
		
		System.out.println("------------");
		if (!TestSQLTypes.mode) {
			analyzeHelper(sinkOp, 0);
		}
		
		//FunctionDependencyTest.printInfo();
	}
	
	public static void printSop(int level, SOperator sop) {
		println(level, sop.op.getClass().toString());
		//println(level, sop);
		println(level, sop.prettyString());
		println();
		
		for (SOperator op: sop.parents) {
			printSop(level+1, op);
		}
	}

	//main work
	@SuppressWarnings("unchecked")
	public static void analyzeHelper(Operator sinkOp, int level) {
		
		println(level, sinkOp.getClass());
		if (sinkOp instanceof TableScanOperator) {
			//System.out.println("=========== " + opParseCtx.get(sinkOp).getRowResolver().tableOriginalName);
			
			//System.out.println("========= " + ((TableScanOperator)(sinkOp)).getNeededColumnIDs());
			//System.out.println("========= " + ((TableScanOperator)(sinkOp)).getNeededColumns());
			//System.out.println("======Table Desc " + ((TableScanOperator)(sinkOp)).getTableDesc());
			//System.out.println(qb.getTabNameForAlias("a"));
			//System.out.println(qb.getTabNameForAlias("b"));
		}

		println(level, "Column Expr Map: ");
		
		Map<String, ExprNodeDesc> map = sinkOp.getColumnExprMap();
		if (map != null && map.entrySet() != null) {
			for (Entry<String, ExprNodeDesc> entry: map.entrySet()) {
				if (entry.getValue() instanceof ExprNodeColumnDesc) {
					println(level, entry.getKey() + ": " 
						+ ((ExprNodeColumnDesc)entry.getValue()).getTabAlias()
						+ ((ExprNodeColumnDesc)entry.getValue()).getCols());
				} else if (entry.getValue() instanceof ExprNodeConstantDesc) {
					println(level, entry.getKey() + ":: " 
							+ ((ExprNodeConstantDesc)entry.getValue()).getExprString());
							//+ ((ExprNodeConstantDesc)entry.getValue()).getCols());
				} else {
					println(level, entry.getValue().getExprString());
					//throw new RuntimeException("ExprNode Type does not supported!");
				}
			}
		}
		
		println(level, "Schema: ");
		RowSchema schema = sinkOp.getSchema();
		for (ColumnInfo info: schema.getSignature()) {
			println(level, info.getTabAlias() + "[" + info.getInternalName() + "]");
		}

		
		if (sinkOp instanceof JoinOperator) {

			//println(level, ((JoinOperator) sinkOp).getPosToAliasMap());
			//println(level, "Reversed Mapping: " + ((JoinOperator)sinkOp).getConf().getReversedExprs());
			//println(level, ((JoinOperator)sinkOp).getConf());


			//for (ExprNodeDesc nodeDesc: ((JoinOperator)sinkOp).getConf().getExprs()) {}
			//println(level, ((JoinOperator)sinkOp).getColumnExprMap());

			//for exprs
			/*
			for (List<ExprNodeDesc> lst : ((JoinOperator)sinkOp).getConf().getExprs().values()) {
				printLevel(level);
				for (ExprNodeDesc desc: lst) {
					print(((ExprNodeColumnDesc)desc).getTabAlias() + " " + ((ExprNodeColumnDesc)desc).getCols());
				}
				println();
			}

			//for filters
			for (List<ExprNodeDesc> lst : ((JoinOperator)sinkOp).getConf().getFilters().values()) {
				printLevel(level);
				//print(((JoinOperator)sinkOp).getConf().getFilters());
				for (ExprNodeDesc desc: lst) {
					print(desc.getClass() + " ");
					//print(((ExprNodeColumnDesc)desc).getTabAlias() + " " + ((ExprNodeColumnDesc)desc).getCols());
				}
				println();
			}

			println(level, "output");

			println(level, ((JoinOperator)sinkOp).getConf().getOutputColumnNames());
			 */

			//println(level, ((JoinOperator)sinkOp).getConf().getExprsStringMap());
		}

		if (sinkOp instanceof ReduceSinkOperator) {
			//println(level, ((ReduceSinkOperator)sinkOp).getConf().getOutputKeyColumnNames());
			/*
			for (ExprNodeDesc desc: ((ReduceSinkOperator)sinkOp).getConf().getValueCols()) {
				println(level, ((ExprNodeColumnDesc)desc).getTabAlias() + " "
								+ ((ExprNodeColumnDesc)desc).getCols());
			}
			 */

		}

		if (sinkOp instanceof SelectOperator) {
			/*
			for (ExprNodeDesc desc: ((SelectOperator)sinkOp).getConf().getColList()) {
				println(level, ((ExprNodeColumnDesc)desc).getTabAlias() + " "
								+ ((ExprNodeColumnDesc)desc).getCols());
			}*/
			//println(level, ((SelectOperator)sinkOp).getConf().getColList());
			//println(level, ((SelectOperator)sinkOp).getConf().getOutputColumnNames());
		}

		if (sinkOp instanceof TableScanOperator) {
			//TableScanDesc desc = ((TableScanOperator)sinkOp).getConf();
			//println(level, desc.getAlias());
			
			//println(level, desc.getFilterExpr());
			//println(level, desc.getBucketFileNameMapping());
			//println(level, desc.getVirtualCols());
			//println(level, desc.getPartColumns());
		}

		if (sinkOp instanceof FilterOperator) {
			println(level, ((FilterOperator)sinkOp).getConf().getPredicate().getExprString());
			//ExprNodeDesc desc = ((FilterOperator)sinkOp).getConf().getPredicate();
			//(ExprNodeGenericFuncDesc)((FilterOperator)sinkOp).getConf().getPredicate()
			//println(level, ((ExprNodeGenericFuncDesc)desc).getExprString());
			//println(level, ((ExprNodeGenericFuncDesc)desc).getCols());
		}
		
		if (sinkOp instanceof LimitOperator) {
			println(level, ((LimitOperator)sinkOp).getConf().getClass());
			//ExprNodeDesc desc = ((FilterOperator)sinkOp).getConf().getPredicate();
			//(ExprNodeGenericFuncDesc)((FilterOperator)sinkOp).getConf().getPredicate()
			//println(level, ((ExprNodeGenericFuncDesc)desc).getExprString());
			//println(level, ((ExprNodeGenericFuncDesc)desc).getCols());
		}

		List<Operator> lst = sinkOp.getParentOperators();
		if (lst != null) {
			for (Operator l: lst) {
				analyzeHelper(l, level + 1);
			}
		}

	}

	/**
	 * called from {@link ColumnPrunerProcFactory#getTableScanProc()}
	 */
	public static void processTableName(TableScanOperator scanOp, RowResolver inputRR) {
		//System.out.println("======InputRR======" + inputRR.tableOriginalName);
		//System.out.println(inputRR.getRslvMap());
	}
	
	/**
	 * called from {@link SemanticAnalyzer#genTablePlan}
	 */
	public static void genTableName(RowResolver rwsch, Table tab) {
		rwsch.tableOriginalName = tab.getTableName();
		System.out.println("======Gen Table Name===== " + rwsch.tableOriginalName);
	}
	
	public static void println(int level, Object content) {
		for (int i=0; i< level; i++) {
			System.out.print("  ");
		}
		System.out.println(content);
	}

	public static void println() {
		System.out.println();
	}

	public static void printLevel(int level) {
		for (int i=0; i< level; i++) {
			System.out.print("  ");
		}
	}

	public static void print(Object content) {
		System.out.print(content + " ");
	}

}
