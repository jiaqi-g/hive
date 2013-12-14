package org.apache.hadoop.hive.ql.cs;

import java.util.*;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;

class FD {
	Set<String> determinists;
	Set<String> dependents;
	
	public FD(Set<String> determinists, Set<String> dependents) {
		this.determinists = determinists;
		this.dependents = dependents;
	}
	
	public static Set<String> infer(Set<String> det, Set<FD> rules) {
		boolean flag = false;
		Set<String> ret = new HashSet<String>(det);
		do
		{
			flag = false;
			for (FD rule : rules) {
				if (ret.containsAll(rule.determinists)) {
					ret.addAll(rule.dependents);
					flag = true;
				}
			}
		} while (flag);
		return ret;
	}
	
	public static boolean judge(Set<String> inferred, Set<String> originals) {
		if (inferred.containsAll(originals)) {
			return true;
		} else {
			return false;
		}
	}
}

public class FunctionDependencyTest<T> {
	
	public FD doTests(SOperator sop) {

		return null;
		/*
		FD fd = new FD();
		
		//parents' FD first
		if (sop.parents.size() > 0) {
			for (SOperator parent: sop.parents) {
				fd.addAll(doTests(parent));
			}
		}

		Operator op = sop.op;
		if (op instanceof TableScanOperator) {
			fd.addAll(sop.rootFD);
		}
		
		//repeat adding dependencies to FD set until no longer changed 
		boolean changed = true;
		while (changed) {
			changed = false;
			
			
		}

		Set<T> results = new HashSet<T>();

		if (stat.dependency.conditions == null || conditions.containsAll(stat.dependency.conditions)) {
			results.addAll(stat.dependency.results);
			conditions.addAll(stat.dependency.results);
		}
		 */
	}
	
	public static void printInfo() {
		System.out.println("----------Calling FunctionDependencyTest-------------------");
	}

}