package org.apache.hive.dynsql;


import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.service.cli.RowSet;

import java.util.Iterator;

public interface DynamicSQLProvider extends Iterator<DynamicSQL> {

    void handleResult(RowSet rows) throws HiveException;
}
