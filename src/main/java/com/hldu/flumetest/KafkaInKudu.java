package com.hldu.flumetest;

import com.google.common.collect.ImmutableMap;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.flume.sink.KuduOperationsProducer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author ：dhl
 * @date ： 2019/11/25 15:04
 * @description：
 * @version:
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KafkaInKudu implements KuduOperationsProducer {

    private static Logger logger = LoggerFactory.getLogger(KafkaInKudu.class);

    @Override
    public void initialize(KuduTable kuduTable) {
        logger.info("正在初始化数据------------");
        Schema schema = kuduTable.getSchema();
        List<ColumnSchema> listColumn = schema.getColumns();
        for (ColumnSchema temp:listColumn){
            logger.info(temp.getName()+"---"+temp.getType());
        }
    }

    @Override
    public List<Operation> getOperations(Event event) {
        try {
            logger.info(new String(event.getBody(),"utf-8"));
            //TODO 此处实现自己的业务逻辑
        }catch (Exception e){
            logger.error(e.getMessage(),e);
        }
        return null;
    }

    @Override
    public void close() {
        logger.info("正在关闭----------------------");
    }

    @Override
    public void configure(Context context) {
        logger.info("配置-----------------------");
        ImmutableMap<String, String> map = context.getParameters();
        for (Map.Entry entry:map.entrySet()){
            logger.info(entry.getKey()+"------"+entry.getValue());
        }
    }
}
