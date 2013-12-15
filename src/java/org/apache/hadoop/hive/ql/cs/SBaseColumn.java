package org.apache.hadoop.hive.ql.cs;

public class SBaseColumn extends SAbstractColumn {

	/**
	 * construct from name and tableAlias
	 * @return
	 */
	public SBaseColumn(String name, String tableAlias) {
		super(name, tableAlias);
	}
	
	public String getName() {
		return name;
	}

	public String toString() {
		return "BASE: " + "(" + tableAlias +")"+ "[" + name + "]";
	}
	

	@Override
	public boolean isGeneratedByAggregate() {
		return false;
	}
}