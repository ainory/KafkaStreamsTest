package com.ainory.kafka.streams.entity;

import com.ainory.kafka.streams.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.LongSummaryStatistics;

/**
 * @author ainory on 2018. 3. 20..
 */
public class HostMetricVO implements Serializable{

    private final String DATE_FORMAT_COMMON = "yyyy-MM-dd HH:mm:ss";
    private final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("##0.00");
    private String hostname;

    private long startTimestamp = 0L;
    private long endTimestamp = 0L;
    private long summaryTimestamp = 0L;

    private String startTimestampStr;
    private String endTimestampStr;

    private ArrayList<Double> cpu_list = new ArrayList<>();
    private ArrayList<Long> memory_list = new ArrayList<>();

    private double cpu_avg;
    private double cpu_min;
    private double cpu_max;

    private double memory_avg;
    private long memory_min;
    private long memory_max;

    public double interval;

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
        this.startTimestampStr = DateFormatUtils.format(startTimestamp, DATE_FORMAT_COMMON);
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
        this.endTimestampStr = DateFormatUtils.format(endTimestamp, DATE_FORMAT_COMMON);
    }

    public long getSummaryTimestamp() {
        return summaryTimestamp;
    }

    public void setSummaryTimestamp(long summaryTimestamp) {
        this.summaryTimestamp = summaryTimestamp;
    }

    public double getCpu_avg() {
        return cpu_avg;
    }

    public void setCpu_avg(double cpu_avg) {
        this.cpu_avg = cpu_avg;
    }

    public double getCpu_min() {
        return cpu_min;
    }

    public void setCpu_min(double cpu_min) {
        this.cpu_min = cpu_min;
    }

    public double getCpu_max() {
        return cpu_max;
    }

    public void setCpu_max(double cpu_max) {
        this.cpu_max = cpu_max;
    }

    public double getMemory_avg() {
        return memory_avg;
    }

    public void setMemory_avg(double memory_avg) {
        this.memory_avg = memory_avg;
    }

    public long getMemory_min() {
        return memory_min;
    }

    public void setMemory_min(long memory_min) {
        this.memory_min = memory_min;
    }

    public long getMemory_max() {
        return memory_max;
    }

    public void setMemory_max(long memory_max) {
        this.memory_max = memory_max;
    }

    public ArrayList<Double> getCpu_list() {
        return cpu_list;
    }

    public void setCpu_list(ArrayList<Double> cpu_list) {
        this.cpu_list = cpu_list;
    }

    public ArrayList<Long> getMemory_list() {
        return memory_list;
    }

    public void setMemory_list(ArrayList<Long> memory_list) {
        this.memory_list = memory_list;
    }

    public double getInterval() {
        return interval;
    }

    public void setInterval(double interval) {
        this.interval = interval;
    }

    public void cpu_calc(double val){
        DoubleSummaryStatistics statistics = new DoubleSummaryStatistics();

        cpu_list.add(val);

        for(double cpu_val : cpu_list){
            statistics.accept(cpu_val);
        }

        setCpu_avg(statistics.getAverage());
        setCpu_min(statistics.getMin());
        setCpu_max(statistics.getMax());
    }

    public void memory_calc(long val){

        LongSummaryStatistics statistics = new LongSummaryStatistics();

        memory_list.add(val);

        for(long memory_val : memory_list){
            statistics.accept(memory_val);
        }

        setMemory_avg(statistics.getAverage());
        setMemory_min(statistics.getMin());
        setMemory_max(statistics.getMax());

    }

    public String toCpuStatisticsCollectdJson(){
        return toCollectdJsonString(hostname, "cpu", "percent", "cpu_idle", cpu_avg, cpu_min, cpu_max, interval, System.currentTimeMillis());
    }

    public String toMemoryStatisticsCollectdJson(){
        return toCollectdJsonString(hostname, "memory", "memory", "used", memory_avg, memory_min, memory_max, interval, System.currentTimeMillis());
    }

    private  String toCollectdJsonString(String host, String plugin, String type, String type_instance, Number avg_value, Number min_value, Number max_value, double interval, long time){

        ArrayList<CollectdKafkaVO> list = new ArrayList<>();

        CollectdKafkaVO collectdKafkaVO = setCollectdKafkaVO(host, plugin, type, type_instance, interval, time);
        collectdKafkaVO.setType_instance(type_instance+"_avg");
        if(StringUtils.equals(plugin, "memory")){
            collectdKafkaVO.setValues(new ArrayList<Long>(){{add(avg_value.longValue());}});
        }else{
            collectdKafkaVO.setValues(new ArrayList<Double>(){{add(avg_value.doubleValue());}});
        }

        list.add(collectdKafkaVO);

        collectdKafkaVO = setCollectdKafkaVO(host, plugin, type, type_instance, interval, time);
        collectdKafkaVO.setType_instance(type_instance+"_min");
        if(StringUtils.equals(plugin, "memory")){
            collectdKafkaVO.setValues(new ArrayList<Long>(){{add(min_value.longValue());}});
        }else{
            collectdKafkaVO.setValues(new ArrayList<Double>(){{add(min_value.doubleValue());}});
        }

        list.add(collectdKafkaVO);

        collectdKafkaVO = setCollectdKafkaVO(host, plugin, type, type_instance, interval, time);
        collectdKafkaVO.setType_instance(type_instance+"_max");
        if(StringUtils.equals(plugin, "memory")){
            collectdKafkaVO.setValues(new ArrayList<Long>(){{add(max_value.longValue());}});
        }else{
            collectdKafkaVO.setValues(new ArrayList<Double>(){{add(max_value.doubleValue());}});
        }

        list.add(collectdKafkaVO);

        return JsonUtil.objectToJsonString(list);
    }

    private CollectdKafkaVO setCollectdKafkaVO(String host, String plugin, String type, String type_instance, double interval, long time){
        CollectdKafkaVO collectdKafkaVO = new CollectdKafkaVO();

        collectdKafkaVO.setHost(host);
        collectdKafkaVO.setDsnames(new ArrayList<String>(){{add("value");}});
        collectdKafkaVO.setDstypes(new ArrayList<String>(){{add("gauge");}});
        collectdKafkaVO.setTime(String.valueOf(time));
        collectdKafkaVO.setInterval(interval);
        collectdKafkaVO.setPlugin(plugin);
        collectdKafkaVO.setPlugin_instance("");
        collectdKafkaVO.setType(type);
        collectdKafkaVO.setMeta(new HashMap<String,Boolean>(){{put("network: received", true);}});

        return collectdKafkaVO;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("hostname='").append(hostname).append('\'');
        sb.append(", startTimestamp=").append(startTimestamp);
        sb.append(", endTimestamp=").append(endTimestamp);
        sb.append(", startTimestampStr=").append(startTimestampStr);
        sb.append(", endTimestampStr=").append(endTimestampStr);
//        sb.append(", cpu_list=").append(cpu_list);
//        sb.append(", memory_list=").append(memory_list);
        sb.append(", cpu_avg=").append(DECIMAL_FORMAT.format(cpu_avg));
        sb.append(", cpu_min=").append(DECIMAL_FORMAT.format(cpu_min));
        sb.append(", cpu_max=").append(DECIMAL_FORMAT.format(cpu_max));
        sb.append(", memory_avg=").append(DECIMAL_FORMAT.format(memory_avg));
        sb.append(", memory_min=").append(memory_min);
        sb.append(", memory_max=").append(memory_max);
        return sb.toString();
    }

    public static void main(String[] args) {

        HostMetricVO hostMetricVO = new HostMetricVO();

        System.out.println(hostMetricVO.toCollectdJsonString("spanal-app", "cpu", "percent", "cpu_idle", 90.00, 89.00, 91.00, 900, System.currentTimeMillis()));

    }
}
