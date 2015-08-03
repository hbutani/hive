package org.apache.hive.dynsql;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.service.cli.RowSet;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TestDynamicSQLProvider implements DynamicSQLProvider {

    private final Log LOG = LogFactory.getLog(TestDynamicSQLProvider.class.getName());

    static List<DynamicSQL> stats = Arrays.asList(
            new DynamicSQL("insert into part2 select * from part", false),
            new DynamicSQL("select count(*) from part2", true)
    );

    Iterator<DynamicSQL> it = stats.iterator();

    @Override
    public void handleResult(RowSet rows) throws HiveException {
        Iterator<Object[]> rIt = rows.iterator();
        StringBuilder buf = new StringBuilder();

        while(rIt.hasNext()) {
            Object[] row = rIt.next();
            for(int  i=0; i < row.length; i++) {
                buf.append(row[i].toString());
                buf.append(",");
            }
            buf.append(System.getProperty("line.separator"));
        }

        LOG.info(String.format("\nSQL Result:\n %s", buf));
    }

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public DynamicSQL next() {
        return it.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
