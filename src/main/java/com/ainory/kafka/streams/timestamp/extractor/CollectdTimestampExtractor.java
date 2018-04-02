package com.ainory.kafka.streams.timestamp.extractor;

import com.ainory.kafka.streams.util.JsonUtil;
import com.ainory.kafka.streams.entity.CollectdKafkaVO;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * @author ainory on 2018. 3. 29..
 */
public class CollectdTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {

        try {
            CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(consumerRecord.value().toString(), CollectdKafkaVO[].class);
            CollectdKafkaVO collectdKafkaVO = arrCollectdKafkaVO[0];

            try {
                return Long.parseLong(StringUtils.replace(collectdKafkaVO.getTime(), ".", ""));
            } catch (Exception e) {
                e.printStackTrace();
                return System.currentTimeMillis();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return System.currentTimeMillis();
        }
    }
}
