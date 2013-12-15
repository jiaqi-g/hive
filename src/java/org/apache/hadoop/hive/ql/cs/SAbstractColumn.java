package org.apache.hadoop.hive.ql.cs;

public abstract class SAbstractColumn {
	protected String name;
	protected String tableAlias;
	
	public SAbstractColumn(String name, String tableAlias) {
		this.name = ((name == null) ? "" : name);
		this.tableAlias = ((tableAlias == null) ? "" : tableAlias);
	}
	
	public boolean equals(String name, String tableAlias) {
		return this.name.equals(name) && this.tableAlias.equals(tableAlias);
	}
	
	public abstract boolean isGeneratedByAggregate();
	
	@Override
	public String toString() {
		return tableAlias + "[" + name + "]";
	}
}