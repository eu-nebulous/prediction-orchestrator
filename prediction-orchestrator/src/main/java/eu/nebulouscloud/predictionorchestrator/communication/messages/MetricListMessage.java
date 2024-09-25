package eu.nebulouscloud.predictionorchestrator.communication.messages;


import java.util.List;

import lombok.Data;

@Data
public class MetricListMessage {
    private String name;
    private long version;
    private List<Metric> metricList;

    public MetricListMessage(String name, long version, List<Metric> metricList) {
        this.name = name;
        this.version = version;
        this.metricList = metricList;
    }
}