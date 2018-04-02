package com.ainory.kafka.streams.keyvalue.mapper;

import com.ainory.kafka.streams.entity.CollectdKafkaVO;
import com.ainory.kafka.streams.entity.DslHostMetricVO;
import com.ainory.kafka.streams.util.JsonUtil;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.math.BigDecimal;

/**
 * @author ainory on 2018. 3. 30..
 */
public class DslKeyValueMapper implements KeyValueMapper<String, String, KeyValue<String, DslHostMetricVO>> {

    @Override
    public KeyValue<String, DslHostMetricVO> apply(String key, String value) {


        CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(value, CollectdKafkaVO[].class);
        CollectdKafkaVO collectdKafkaVO = arrCollectdKafkaVO[0];

        StringBuffer resultKey = new StringBuffer();
        if (collectdKafkaVO.getDsnames().get(0).equals("value")) {
            // make key - single value ( ex -> [{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1522299234.188,"interval":10.000,"host":"spanal-3","plugin":"disk","plugin_instance":"dm-2","type":"pending_operations","type_instance":"","meta":{"network:received":true}}] )
            resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType()).append("^").append(collectdKafkaVO.getType_instance());


            DslHostMetricVO dslHostMetricVO = new DslHostMetricVO();

            dslHostMetricVO.setHostname(collectdKafkaVO.getHost());

            if (collectdKafkaVO.getValues().get(0) instanceof Integer) {
                dslHostMetricVO.setValue(new BigDecimal(new Integer((Integer) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMin(new BigDecimal(new Integer((Integer) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMax(new BigDecimal(new Integer((Integer) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setAvg(new BigDecimal(new Integer((Integer) collectdKafkaVO.getValues().get(0))));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Long) {
                dslHostMetricVO.setValue(new BigDecimal(new Long((Long) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMin(new BigDecimal(new Long((Long) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMax(new BigDecimal(new Long((Long) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setAvg(new BigDecimal(new Long((Long) collectdKafkaVO.getValues().get(0))));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Float) {
                dslHostMetricVO.setValue(new BigDecimal(new Float((Float) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMin(new BigDecimal(new Float((Float) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMax(new BigDecimal(new Float((Float) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setAvg(new BigDecimal(new Float((Float) collectdKafkaVO.getValues().get(0))));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Double) {
                dslHostMetricVO.setValue(new BigDecimal(new Double((Double) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMin(new BigDecimal(new Double((Double) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMax(new BigDecimal(new Double((Double) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setAvg(new BigDecimal(new Double((Double) collectdKafkaVO.getValues().get(0))));
            } else {
                dslHostMetricVO.setValue(new BigDecimal(0));
                dslHostMetricVO.setMin(new BigDecimal(0));
                dslHostMetricVO.setMax(new BigDecimal(0));
                dslHostMetricVO.setAvg(new BigDecimal(0));
            }

            return new KeyValue<>(resultKey.toString(), dslHostMetricVO);

        } else {
            // make key - multi value ( ex -> [{"values":[0,0.699869398389539],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1522299234.188,"interval":10.000,"host":"spanal-3","plugin":"disk","plugin_instance":"dm-2","type":"disk_time","type_instance":"","meta":{"network:received":true}}] )
//                            resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType()).append("^").append(collectdKafkaVO.getType_instance());
            resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType());
            resultKey.append("_").append(collectdKafkaVO.getDsnames().get(0));

            DslHostMetricVO dslHostMetricVO = new DslHostMetricVO();
            dslHostMetricVO.setHostname(collectdKafkaVO.getHost());

            if (collectdKafkaVO.getValues().get(0) instanceof Integer) {
                dslHostMetricVO.setValue(new BigDecimal(new Integer((Integer) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMin(new BigDecimal(new Integer((Integer) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMax(new BigDecimal(new Integer((Integer) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setAvg(new BigDecimal(new Integer((Integer) collectdKafkaVO.getValues().get(0))));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Long) {
                dslHostMetricVO.setValue(new BigDecimal(new Long((Long) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMin(new BigDecimal(new Long((Long) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMax(new BigDecimal(new Long((Long) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setAvg(new BigDecimal(new Long((Long) collectdKafkaVO.getValues().get(0))));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Float) {
                dslHostMetricVO.setValue(new BigDecimal(new Float((Float) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMin(new BigDecimal(new Float((Float) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMax(new BigDecimal(new Float((Float) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setAvg(new BigDecimal(new Float((Float) collectdKafkaVO.getValues().get(0))));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Double) {
                dslHostMetricVO.setValue(new BigDecimal(new Double((Double) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMin(new BigDecimal(new Double((Double) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setMax(new BigDecimal(new Double((Double) collectdKafkaVO.getValues().get(0))));
                dslHostMetricVO.setAvg(new BigDecimal(new Double((Double) collectdKafkaVO.getValues().get(0))));
            } else {
                dslHostMetricVO.setValue(new BigDecimal(0));
                dslHostMetricVO.setMin(new BigDecimal(0));
                dslHostMetricVO.setMax(new BigDecimal(0));
                dslHostMetricVO.setAvg(new BigDecimal(0));
            }

            return new KeyValue<>(resultKey.toString(), dslHostMetricVO);
        }
    }
}
