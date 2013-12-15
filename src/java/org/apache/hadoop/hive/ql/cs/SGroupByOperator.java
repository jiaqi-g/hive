package org.apache.hadoop.hive.ql.cs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;

/**
 * Group By Operator will probably generate new columns, if there are "having" after it
 * Group By column will use column name such as "key" "value" constructed from the previous operators,
 * without considering the table name
 * 
 * Having is represented as a filter
 * 
 * @author victor
 *
 */
public class SGroupByOperator extends SOperator {

	GroupByDesc groupByDesc;
	boolean isDistinct = false;
	ArrayList<SAggregate> aggregators = null;
	ArrayList<SDerivedColumn> keys = null;
	
	public SGroupByOperator(GroupByOperator op) {
		super(op);
		groupByDesc = op.getConf();
		aggregators = new ArrayList<SAggregate>();
		keys = new ArrayList<SDerivedColumn>();
		
		setupKeys();
		setupAggregates();
		
		if ((aggregators == null) || (aggregators.size() == 0)) {
			isDistinct = true;
		}
	}
	
	private void setupAggregates() {
		for (AggregationDesc aggr: groupByDesc.getAggregators()) {
			aggregators.add(new SAggregate(aggr, this));
		}
	}
	
	private void setupKeys() {
		for (ExprNodeDesc key: groupByDesc.getKeys()) {
			keys.add(SOperatorFactory.generateSColumnFromDesc(key, this));
		}
	}
	
	@Override
	public void setColumnMap() {
		/**
		 * postMap maps from ExprNodeDesc -> SColumn
		 */
		Map<ExprNodeDesc, SColumn> postMap = new HashMap<ExprNodeDesc, SColumn>();
		
		//parents all columns, since table alias only stores in col
		List<SColumn> parentsAllColumns = new ArrayList<SColumn>();
		for (SOperator p: parents) {
			parentsAllColumns.addAll(p.columns);
		}
		
		Map<String, ExprNodeDesc> columnExprMap = op.getColumnExprMap();
		//only maps ExprNodeColumnDesc type
		for (ExprNodeDesc desc: columnExprMap.values()) {
			if (desc instanceof ExprNodeColumnDesc) {
				for (SColumn parentScol: parentsAllColumns) {
					//special
					if (parentScol.name.equals(((ExprNodeColumnDesc)desc).getColumn())) {
                        postMap.put(desc, parentScol);
                        break;
					}
				}
			} else {
				//do nothing
			}
		}

		int index = 0;
		//generate final mapping
		for (SColumn col: columns) {
			ExprNodeDesc desc = columnExprMap.get(col.getName());
			
			if (desc == null) {
				columnMap.put(col, aggregators.get(index));
				System.out.println("=====================" + aggregators + " " + col);
				index++;
			}
			else {
				SAbstractColumn correspond = postMap.get(desc);
				if (correspond == null) {
					correspond = SOperatorFactory.generateSColumnFromDesc(columnExprMap.get(col.getName()), this);
				}
				columnMap.put(col, correspond);
			}
		}
	}
	
	public SAbstractColumn getRootColumn(SColumn scol) {
		//recursively call its parents until rootColumn or null
		SAbstractColumn key = columnMap.get(scol);
		for (SOperator parent: parents) {
			if (key instanceof SColumn) {
				if (parent.columnMap.containsKey( key )) {
					return parent.getRootColumn( (SColumn) key );
				}
			}
		}
		
		return key;
	}
	
	@Override
	public String prettyString() {
		String s = super.prettyString() + " Group By: ";
		if (groupByDesc.getAggregators() != null) {
			s += "Aggregators: ";
			for (AggregationDesc desc: groupByDesc.getAggregators()) {
				s += desc.getExprString() + " ";
			}
		}

		if (groupByDesc.getKeys() != null) {
			s += "Keys: ";
			for (ExprNodeDesc key: groupByDesc.getKeys()) {
				s += key.getExprString() + " ";
			}
		}
		
		return s;
	}
}