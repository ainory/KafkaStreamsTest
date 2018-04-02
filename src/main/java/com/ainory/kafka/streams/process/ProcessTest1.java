package com.ainory.kafka.streams.process;

import com.ainory.kafka.streams.entity.HostMetricVO;
import com.ainory.kafka.streams.entity.CollectdKafkaVO;
import com.ainory.kafka.streams.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Calendar;

/**
 * @author ainory on 2018. 3. 19..
 */
public class ProcessTest1 extends AbstractProcessor<String, String>{

    private ProcessorContext processorContext;
    private KeyValueStore<String, HostMetricVO> store;

    private final long CHECK_INTERVAL_SEC = 1;

    private final long COLLECTD_COLLECT_INTERVAL_SEC = 10;
    private final int SUMMARY_INTERVAL_SEC = 60;

    private long summaryCheckStartTime = 0;

    @Override
    public void init(ProcessorContext context) {

        this.processorContext = context;
        this.store = (KeyValueStore) processorContext.getStateStore("HostMetric");

        // zero seconds start
        while(true){
            Calendar calendar = Calendar.getInstance();
            if(calendar.get(Calendar.SECOND) == 0){
                break;
            }
            try {
                Thread.sleep(100);
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        // registered schedule
        this.processorContext.schedule(CHECK_INTERVAL_SEC*1000, PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
            @Override
            public void punctuate(long l) {

                if(summaryCheckStartTime == 0){
                    summaryCheckStartTime = getStartTime(l, SUMMARY_INTERVAL_SEC);
                    System.out.println("First summaryCheckTime Setting : "+ DateFormatUtils.format(summaryCheckStartTime, "yyyy-MM-dd HH:mm:ss"));
                    return;
                }

                long checkTime = summaryCheckStartTime + (SUMMARY_INTERVAL_SEC*1000);

                if(checkTime > l){
//                    System.out.println(DateFormatUtils.format((summaryCheckTime + (SUMMARY_INTERVAL_SEC*1000)), "yyyy-MM-dd HH:mm:ss") + " : " + DateFormatUtils.format(l, "yyyy-MM-dd HH:mm:ss"));
                    return;
                }
//                System.out.println("!!! " + DateFormatUtils.format((summaryCheckStartTime + (SUMMARY_INTERVAL_SEC*1000)), "yyyy-MM-dd HH:mm:ss") + " : " + DateFormatUtils.format(l, "yyyy-MM-dd HH:mm:ss"));

                KeyValueIterator<String, HostMetricVO> iter = store.all();
                ArrayList<String> keyList = new ArrayList<>();
                String key = "";
                while(iter.hasNext()){
                    key = iter.next().key;
                    keyList.add(key);

                    HostMetricVO hostMetricVO = store.get(key);

                    if(hostMetricVO == null){
                        continue;
                    }

                    hostMetricVO.setInterval(SUMMARY_INTERVAL_SEC*1.0);
//                    hostMetricVO.setSummaryTimestamp(summaryCheckStartTime*0.001);

                    // send next topic
                    processorContext.forward(DateFormatUtils.format(l, "yyyy-MM-dd HH:mm:ss"), hostMetricVO.toCpuStatisticsCollectdJson());
                    processorContext.forward(DateFormatUtils.format(l, "yyyy-MM-dd HH:mm:ss"), hostMetricVO.toMemoryStatisticsCollectdJson());
                }

                // delete store data
                for(String host_key : keyList){
                    store.delete(host_key);
                }

                System.out.println(DateFormatUtils.format(summaryCheckStartTime, "yyyy-MM-dd HH:mm:ss") + " punctuate end..");
                summaryCheckStartTime = getStartTime(l, SUMMARY_INTERVAL_SEC);
            }

            public  long getStartTime(long l, int prevSec) {
                int itv = prevSec*1000;
                long ct = l - (prevSec*1000);
                long nct = ((ct/itv)*itv)+itv;
                return nct;
            }
        });
    }

    @Override
    public void process(String key, String value) {

        CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(value, CollectdKafkaVO[].class);
        CollectdKafkaVO collectdKafkaVO = arrCollectdKafkaVO[0];

        if(!(StringUtils.equals(collectdKafkaVO.getPlugin(), "cpu") || StringUtils.equals(collectdKafkaVO.getPlugin(), "memory"))){
            return;
        }

        // cpu
        if(StringUtils.equals(collectdKafkaVO.getPlugin(), "cpu")
                && StringUtils.equals(collectdKafkaVO.getType(), "percent")
                && StringUtils.equals(collectdKafkaVO.getType_instance(), "idle")){

            String host = collectdKafkaVO.getHost();

            HostMetricVO hostMetricVO =  this.store.get(host);

            if(hostMetricVO == null){
                hostMetricVO = new HostMetricVO();
            }

            if(hostMetricVO.getStartTimestamp() == 0L){
                hostMetricVO.setStartTimestamp(Long.parseLong(StringUtils.replace(collectdKafkaVO.getTime(), ".","")));
            }
            hostMetricVO.setEndTimestamp(Long.parseLong(StringUtils.replace(collectdKafkaVO.getTime(), ".","")));

            hostMetricVO.setHostname(collectdKafkaVO.getHost());

            double metric_value = Double.parseDouble(String.valueOf(collectdKafkaVO.getValues().get(0)));
            hostMetricVO.cpu_calc(metric_value);

//            System.out.println(hostMetricVO.toString());

            this.store.put(collectdKafkaVO.getHost(), hostMetricVO);
        }

        // memory
        if(StringUtils.equals(collectdKafkaVO.getPlugin(), "memory")
                && StringUtils.equals(collectdKafkaVO.getType(), "memory")
                && StringUtils.equals(collectdKafkaVO.getType_instance(), "used")){

            String host = collectdKafkaVO.getHost();

            HostMetricVO hostMetricVO =  this.store.get(host);

            if(hostMetricVO == null){
                hostMetricVO = new HostMetricVO();
            }

            if(hostMetricVO.getStartTimestamp() == 0L){
                hostMetricVO.setStartTimestamp(Long.parseLong(StringUtils.replace(collectdKafkaVO.getTime(), ".","")));
            }
            hostMetricVO.setEndTimestamp(Long.parseLong(StringUtils.replace(collectdKafkaVO.getTime(), ".","")));

            hostMetricVO.setHostname(collectdKafkaVO.getHost());

            long metric_value = Long.parseLong(String.valueOf(collectdKafkaVO.getValues().get(0)));
            hostMetricVO.memory_calc(metric_value);

//            System.out.println(hostMetricVO.toString());

            this.store.put(collectdKafkaVO.getHost(), hostMetricVO);
        }

        // TODO: ??
//        processorContext.commit();

    }

    @Override
    public void close() {
        store.close();
    }
}
