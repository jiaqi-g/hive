package org.apache.hadoop.hive.ql.cs;

import java.util.List;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;

public class SFunction extends SDerivedColumn {
	ExprNodeGenericFuncDesc desc;
	boolean baseType = true;
	SOperator sop;
	
	public SFunction(ExprNodeGenericFuncDesc desc, SOperator sop) {
		this.desc = desc;
		this.sop = sop;
	}
	
	private void process(ExprNodeGenericFuncDesc desc) {
		List<ExprNodeDesc> descs = desc.getChildExprs();
		
		GenericUDF genericUDF = desc.getGenericUDF();
		if (genericUDF instanceof GenericUDFIf) {
			processIf(descs.get(0), descs.get(1), descs.get(2));
		} else if (genericUDF instanceof GenericUDFCase) {
			for (int i=0; i<descs.size(); i+= 2) {
				processCase(descs.get(i), descs.get(i+1));
			}	
		}
	}
	
	private void processIf(ExprNodeDesc ifClause, ExprNodeDesc thenClause, ExprNodeDesc elseClause) {
		if (thenClause instanceof ExprNodeGenericFuncDesc) {
			process((ExprNodeGenericFuncDesc) thenClause);
		} else if (thenClause instanceof ExprNodeColumnDesc) {
			search((ExprNodeColumnDesc) thenClause);
		}
		
		if (elseClause instanceof ExprNodeGenericFuncDesc) {
			process((ExprNodeGenericFuncDesc) elseClause);
		} else if (elseClause instanceof ExprNodeColumnDesc) {
			search((ExprNodeColumnDesc) elseClause);
		}
	}
	
	private void processCase(ExprNodeDesc caseCondition, ExprNodeDesc caseResult) {
		if (caseResult instanceof ExprNodeGenericFuncDesc) {
			process((ExprNodeGenericFuncDesc) caseResult);
		} else if (caseResult instanceof ExprNodeColumnDesc) {
			search((ExprNodeColumnDesc) caseResult);
		}
	}

	private void search(ExprNodeColumnDesc desc) {
		/**
		 * Notice: search in the parent's schema
		 */
		SOperator parent = sop.parents.get(0);
		
		if (parent.getRootColumn(new SColumn(desc)) instanceof SBaseColumn) {}
		else {
			baseType = false;
		}
	}
	
	@Override
	public boolean isBaseType() {
		process(desc);
		return baseType;
	}
}
