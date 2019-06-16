//package Test;
//
//import com.alibaba.otter.canal.protocol.CanalEntry;
//import com.alibaba.rocketmq.client.consumer.listener.*;
//import com.alibaba.rocketmq.common.message.MessageExt;
//import com.google.protobuf.InvalidProtocolBufferException;
//import org.apache.commons.lang.ArrayUtils;
//import org.apache.kudu.client.KuduException;
//import org.apache.kudu.client.KuduSession;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Service;
//
//import java.util.Calendar;
//import java.util.List;
//import java.util.concurrent.atomic.AtomicInteger;
//
//@Service
//public class
//MessageListenerOrderImp implements MessageListenerOrderly {
//    @Autowired
//    private Write2KuduNew write2Kudu;
//    @Autowired
//    private Write2KuduMars write2KuduMars;
//    @Value("${neeWrite2Table}")
//    private String[] neeWrite2Table;
//    public final static Logger logger = LoggerFactory.getLogger(MessageListenerOrderImp.class);
//    private AtomicInteger atomic = new AtomicInteger(0);//记录当前session中添加多少数据
//    @Value("${operationBatch}")
//    private int operationBatch;
//
//
//    private void incrAndFlush(KuduSession session) {
//        if (atomic.getAndIncrement() >= operationBatch / 2) {//等于缓冲区的一半则flush
//            try {
//                long nowTimeMillis = Calendar.getInstance().getTimeInMillis();
//                session.flush();
//                long flushtime = Calendar.getInstance().getTimeInMillis() - nowTimeMillis;
//                logger.info("flush time : " + flushtime + "  " + "kudu flushed.");
//                atomic.set(0);
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    logger.error("thread sleep error", e.getMessage());
//                }
//            } catch (KuduException e) {
//                logger.error("kudu client flush error.");
//                return;
//                //e.printStackTrace();
//            } finally {
//                try {
//                    session.close();
//                } catch (KuduException e) {
//                    logger.error("kudu client close error.");
//                    e.printStackTrace();
//                }
//            }
//        }
//    }
//
//
//    @Override
//    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext consumeOrderlyContext) {
//        KuduSession session = write2Kudu.getKuduSession();
//        if (session == null) {
//            logger.error("get kudu session error.");
//            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
//        }
//
//        //记录失败任务数量
//        int errs = 0;
//
//        for (int i = 0; i < msgs.size(); i++) {
//            /*try {*/
//            CanalEntry.Entry entry = null;
//            try {
//                entry = CanalEntry.Entry.parseFrom(msgs.get(i).getBody());
//            } catch (InvalidProtocolBufferException e) {
//                logger.info("parse canal data failed!");
//                e.printStackTrace();
//            }
//            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
//                continue;
//            }
//
//            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
//                String tableName = entry.getHeader().getTableName();
//                if (!ArrayUtils.contains(neeWrite2Table, tableName.trim())) {
//                    continue;
//                }
//                CanalEntry.RowChange rowChage = null;
//                try {
//                    rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
//                } catch (Exception e) {
//                    logger.error("parse event has an error , data:" + entry.toString(), e);
//                    e.printStackTrace();
//                }
//
//                CanalEntry.EventType eventType = rowChage.getEventType();
//                if (eventType == CanalEntry.EventType.UPDATE) {
//                    // logger.info("push update data outer" + rowChage.getRowDatasList());
//                    for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
//                        try {
//                            write2Kudu.upsert(tableName, rowData.getAfterColumnsList(), session);
//                            incrAndFlush(session);
//                        } catch (KuduException e) {
//                            errs++;
//                            logger.info("update kudu data failed");
//                            e.printStackTrace();
//                            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
//                        }
//                    }
//                } else if (eventType == CanalEntry.EventType.INSERT) {
//                    for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
//                        try {
//                            write2Kudu.insert(tableName, rowData.getAfterColumnsList(), session);
//                            incrAndFlush(session);
//                        } catch (KuduException e) {
//                            errs++;
//                            logger.info("insert kudu data failed");
//                            e.printStackTrace();
//                            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
//                        }
//                    }
//                }
//            }
//        }
//        if (errs == 0) {
//            return ConsumeOrderlyStatus.SUCCESS;
//        } else {
//            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
//        }
//    }
//}
