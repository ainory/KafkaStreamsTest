package com.ainory.kafka.streams.entity;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.DecimalFormat;

/**
 * @author ainory on 2018. 3. 30..
 */
public class DslHostMetricVO implements Serializable {

    private final String DATE_FORMAT_COMMON = "yyyy-MM-dd HH:mm:ss";
    private final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("##0.###");
    private String hostname;

    private long startTimestamp = 0L;
    private long endTimestamp = 0L;
    private long summaryTimestamp = 0L;

    private String startTimestampStr;
    private String endTimestampStr;

    private BigDecimal value = BigDecimal.ZERO;
    private BigDecimal avg = BigDecimal.ZERO;
    private BigDecimal min = null;
    private BigDecimal max = BigDecimal.ZERO;
    private BigDecimal sum = BigDecimal.ZERO;

    private int aggregationCount = 0;

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

    public String getStartTimestampStr() {
        return startTimestampStr;
    }

    public String getEndTimestampStr() {
        return endTimestampStr;
    }

    public long getSummaryTimestamp() {
        return summaryTimestamp;
    }

    public void setSummaryTimestamp(long summaryTimestamp) {
        this.summaryTimestamp = summaryTimestamp;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }

    public BigDecimal getAvg() {
        return avg;
    }

    public BigDecimal getAvg(BigDecimal sumValue, int aggregationCount) {

        return new BigDecimal(sumValue.doubleValue()/aggregationCount);
    }

    public void setAvg(BigDecimal avg) {
        this.avg = avg;
    }

    public BigDecimal getMin() {
        return min;
    }

    public BigDecimal getMin(BigDecimal leftValue, BigDecimal rightValue) {

        if(rightValue == null){
            return leftValue;
        }

        return leftValue.min(rightValue);
    }

    public void setMin(BigDecimal min) {
        this.min = min;
    }

    public BigDecimal getMax() {
        return max;
    }

    public BigDecimal getMax(BigDecimal leftValue, BigDecimal rightValue) {
        return leftValue.max(rightValue);
    }

    public void setMax(BigDecimal max) {
        this.max = max;
    }

    public BigDecimal getSum() {
        return sum;
    }

    public BigDecimal getSum(BigDecimal leftValue, BigDecimal rightValue) {
        return leftValue.add(rightValue);
    }

    public void setSum(BigDecimal sum) {
        this.sum = sum;
    }

    public int getAggregationCount() {
        return aggregationCount;
    }

    public void setAggregationCount(int aggregationCount) {
        this.aggregationCount = aggregationCount;
    }


    public String toSimpleString(){
        StringBuffer str = new StringBuffer();

        str.append("[").append(startTimestampStr).append(" ~ ").append(endTimestampStr).append("] ");
        str.append("min/max/avg/sum: ").append(DECIMAL_FORMAT.format(min.doubleValue())).append(" / ").append(DECIMAL_FORMAT.format(max.doubleValue())).append(" / ").append(DECIMAL_FORMAT.format(avg.doubleValue())).append(" / ").append(DECIMAL_FORMAT.format(sum.doubleValue())).append(" (aggregation count: ").append(aggregationCount).append(")");
        return str.toString();
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("");
//        sb.append("DATE_FORMAT_COMMON='").append(DATE_FORMAT_COMMON).append('\'');
//        sb.append(", DECIMAL_FORMAT=").append(DECIMAL_FORMAT);
        sb.append("[hostname='").append(hostname).append('\'');
        sb.append(", startTimestamp=").append(startTimestamp);
        sb.append(", endTimestamp=").append(endTimestamp);
        sb.append(", summaryTimestamp=").append(summaryTimestamp);
        sb.append(", startTimestampStr='").append(startTimestampStr).append('\'');
        sb.append(", endTimestampStr='").append(endTimestampStr).append('\'');
        sb.append(", value=").append(value);
        sb.append(", avg=").append(avg);
        sb.append(", min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", sum=").append(sum);
        sb.append(", aggregationCount=").append(aggregationCount);
        sb.append("]");
        return sb.toString();
    }
}
