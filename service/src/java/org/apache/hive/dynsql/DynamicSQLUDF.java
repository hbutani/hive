package org.apache.hive.dynsql;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.apache.hive.service.cli.*;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.SessionManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Description(name = "dynamic_sql",
        value = "_FUNC_(obj0) - "
                + "Given a DYnamicSQLProvider class, it runs the SQLs provided by it.",
        extended = "Example:\n"
                + "  > SELECT _FUNC_('org.apache.hive.dynsql.TestDynamicSQLProvider')")
public class DynamicSQLUDF extends GenericUDF {

    private final Log LOG = LogFactory.getLog(DynamicSQLUDF.class.getName());

    private transient Converter elemConverter;
    private transient Converter szConverter;
    private final List<Object> ret = new ArrayList<Object>();
    private transient ObjectInspector[] argumentOIs;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {


        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        LOG.debug(loader.getResource("org.apache.hive.service.cli.session.SessionManager"));

        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                    "The function DYNAMIC_SQL(<dynSQLCallbackClass>) needs one argument.");
        }

        String dynamicProviderClassName = null;


        if (arguments[0].getCategory() == ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) arguments[0])
                .getPrimitiveCategory() == PrimitiveCategory.STRING
                && (arguments[0] instanceof ConstantObjectInspector)) {
            dynamicProviderClassName =
                    ((ConstantObjectInspector) arguments[0]).getWritableConstantValue().toString();
        } else {
            throw new UDFArgumentException(String.format(
                    "DynamicCallback clasName required for DYNAMIC_SQL invocation"));

        }

        HiveSession sess = SessionManager.getCurrentSessionHandle();

        if ( sess == null ) {
            throw new UDFArgumentException(String.format(
                    "No HiveSession available for DYNAMIC_SQL invocation"));
        }

        try {

            DynamicSQLProvider sqlProvider = null;
            try {
                sqlProvider = DynamicSQLProvider.class.cast(
                        Class.forName(dynamicProviderClassName).newInstance());
            } catch (Exception e) {
                throw new UDFArgumentException(String.format(
                        "%s is not a DynamicSQLProvider class for DYNAMIC_SQL invocation",
                        dynamicProviderClassName));
            }

            DynamicSQL sql = null;
            try {
                while (sqlProvider.hasNext()) {
                    // TODO allow this as an arg?
                    Map<String, String> confOverlay = new HashMap();
                    sql = sqlProvider.next();
                    OperationHandle oH = sess.executeStatement(sql.getSql(), confOverlay);
                    if (sql.isFetchResults()) {
                        RowSet rSet = sess.fetchResults(oH,
                                FetchOrientation.FETCH_NEXT, Long.MAX_VALUE,
                                FetchType.QUERY_OUTPUT);

                        sqlProvider.handleResult(rSet);
                    }
                }
            } catch (HiveSQLException e) {
                throw new UDFArgumentException(e);
            } catch (HiveException he) {
                throw new UDFArgumentException(he);
            }


            return
                    PrimitiveObjectInspectorFactory.
                            getPrimitiveWritableConstantObjectInspector(
                                    TypeInfoFactory.stringTypeInfo,
                                    new Text("Done"));
        } finally {
            SessionManager.reattachSession(sess);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return new Text("Done");
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 1);
        return String.format("dynamic_sql(%s)", children[0]);
    }


}