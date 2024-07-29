package eu.nebulouscloud.predictionorchestrator.messages;

import java.util.List;

import lombok.Data;
import org.json.JSONArray;
import org.json.JSONObject;

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

    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("name", name);
        json.put("version", version);
        JSONArray metricsArray = new JSONArray();
        for (Metric metric : metricList) {
            metricsArray.put(metric.toJson());
        }
        json.put("metric_list", metricsArray);
        return json;
    }
}