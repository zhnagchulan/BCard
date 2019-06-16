//package Test;
//
//import com.alibaba.otter.canal.protocol.CanalEntry;
//import org.apache.kudu.Schema;
//import org.apache.kudu.Type;
//import org.apache.kudu.client.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Service;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//@Service
//public class Write2Kudu {
//    @Autowired
//    private KuduClientConf kdClient;  //单例，创建kudu的数据库链接
//    @Value("${preKudoTabel}")
//    private String preKudoTabel;  //kudo的表名前缀  .
//    //KuduTable  Create a table on the cluster with the specified name and schema.
//    private static Map<String, KuduTable> tables = new HashMap<String, KuduTable>();
//    public final static Logger logger = LoggerFactory.getLogger(Write2Kudu.class);  //使用指定类初始化日志对象
//    private static KuduSession kuduSession;
//    @Value("${operationBatch}")
//    private int operationBatch;
//
//    //处理数据，判断数据类型
//    private static void fillRow(PartialRow row, Type type, int colIdx, CanalEntry.Column column) {
//        switch (type) {
//            case BOOL:
//                row.addBoolean(colIdx,
//                        Boolean.parseBoolean(column.getValue()));
//                break;
//            case FLOAT:
//                row.addFloat(colIdx,
//                        Float.parseFloat(column.getValue()));
//                break;
//            case DOUBLE:
//                row.addDouble(colIdx,
//                        Double.parseDouble(column.getValue()));
//                break;
//            case BINARY:
//                row.addBinary(colIdx, column.getValue()
//                        .getBytes());
//                break;
//            case INT8:
//                row.addByte(colIdx, Byte.parseByte(column.getValue()));
//                break;
//            case INT16:
//                row.addShort(colIdx,
//                        Short.parseShort(column.getValue()));
//                break;
//            case INT32:
//                row.addInt(colIdx,
//                        Integer.parseInt(column.getValue()));
//                break;
//            case INT64:
//                row.addLong(colIdx,
//                        Long.parseLong(column.getValue()));
//                break;
//            case STRING:
//                row.addString(colIdx, column.getValue());
//                break;
//            default:
//                throw new IllegalStateException(String.format(
//                        "unknown column type %s", type));
//        }
//    }
//
//    //创建kudosession
//    public KuduSession getKuduSession() {
//        if (kuduSession == null) {
//            synchronized (Write2KuduNew.class) {
//                if (kuduSession == null) {
//                    try {
//                        kuduSession = newAsyncSession(operationBatch);
//                        logger.info("kudu session created");
//                    } catch (Exception e) {
//                        //logger.error("kuduSessiono_create_error", e);
//                    }
//                }
//            }
//        }
//        return kuduSession;
//    }
//
//
//    public KuduTable table(String name) throws KuduException {
//        KuduTable table = tables.get(name);
//        if (table == null) {
//            synchronized (Write2KuduNew.class) {
//                table = tables.get(name);
//                if (table == null) {
//                    logger.info("create kudu table object,name={}", name);
//                    // Open the table with the given name.
//                    table = kdClient.getKuduClient().openTable(preKudoTabel + name);
//                    tables.put(name, table);
//                }
//            }
//        }
//        return table;
//    }
//
//
//    /**
//     * FlushMode:AUTO_FLUSH_BACKGROUND
//     *
//     * @return
//     * @throws KuduException
//     */
//    public KuduSession newAsyncSession(int bufferSpace) {
//        KuduSession session = kdClient.getKuduClient().newSession();
//        //用于设定清理缓存的时间点，如果FlushMode是MANUAL或NEVEL,在操作过程中hibernate会将事务设置为readonly
//        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
//        //session.setFlushInterval(bufferSpace/2);//milliseconds
//        session.setTimeoutMillis(80000);
//        session.setIgnoreAllDuplicateRows(true);
//        session.setMutationBufferSpace(bufferSpace);
//        return session;
//    }
//
//    public static Upsert createUpsert(KuduTable ktable, List<CanalEntry.Column> columnList) {
//        Upsert upsert = ktable.newUpsert();
//        PartialRow row = upsert.getRow();
//        Schema schema = ktable.getSchema();
//
//        for (CanalEntry.Column column : columnList) {
//            if (column.getValue().length() <= 0) {
//                continue;
//            }
//            String columnName = column.getName();//.toLowerCase();//kudu中表名不支持大写,转为小写
//            int colIdx = schema.getColumnIndex(columnName);
//            Type colType = schema.getColumnByIndex(colIdx).getType();
//            //logger.info(columnName+" "+column+" "+colIdx);
//            fillRow(row, colType, colIdx, column);
//        }
//        return upsert;
//    }
//
//    public static Insert createInsert(KuduTable ktable, List<CanalEntry.Column> columnList) {
//        Insert insert = ktable.newInsert();
//        PartialRow row = insert.getRow();
//        Schema schema = ktable.getSchema();
//
//        for (CanalEntry.Column column : columnList) {
//            if (column.getValue().length() <= 0) {
//                continue;
//            }
//            String columnName = column.getName();//.toLowerCase();//kudu中表名不支持大写,转为小写
//            int colIdx = schema.getColumnIndex(columnName);
//            Type colType = schema.getColumnByIndex(colIdx).getType();
//            //logger.info(columnName+" "+column+" "+colIdx);
//            fillRow(row, colType, colIdx, column);
//        }
//
//        return insert;
//    }
//
//    public void insert(String tableStr, List<CanalEntry.Column> columnList, KuduSession session) throws KuduException {
//        KuduTable table = table(tableStr);
//        Insert insert = createInsert(table, columnList);
//        session.apply(insert);
//    }
//
//    public void upsert(String tableStr, List<CanalEntry.Column> columnList, KuduSession session) throws KuduException {
//        KuduTable table = table(tableStr);
//        Upsert upsert = createUpsert(table, columnList);
//        session.apply(upsert);
//    }
//
//
//}