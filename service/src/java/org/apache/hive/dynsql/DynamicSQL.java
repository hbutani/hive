package org.apache.hive.dynsql;


public class DynamicSQL {
    private String sql;
    private boolean fetchResults;

    public DynamicSQL(String sql, boolean fetchResults) {
        this.sql = sql;
        this.fetchResults = fetchResults;
    }

    public String getSql() {
        return sql;
    }

    public boolean isFetchResults() {
        return fetchResults;
    }
}
