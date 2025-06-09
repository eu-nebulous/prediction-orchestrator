package eu.nebulouscloud.predictionorchestrator;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class Prediction {
    private final double metricValue;
    private final String componentId;
    private final long timestamp;
    private final double probability;
    private final List<Double> confidenceInterval;
    private final long predictionTime;
    private final String predictionMethod;
    private final String metricName;

    public static Map<String, Object> toMap(Prediction prediction) {
        Map<String, Object> predictionMap = new HashMap<>();

        predictionMap.put("metricValue", prediction.getMetricValue());
        predictionMap.put("componentId", prediction.getComponentId());
        predictionMap.put("timestamp", prediction.getTimestamp());
        predictionMap.put("probability", prediction.getProbability());
        predictionMap.put("confidence_interval", prediction.getConfidenceInterval());
        predictionMap.put("predictionTime", prediction.getPredictionTime());
        predictionMap.put("predictionMethod", prediction.getPredictionMethod());
        predictionMap.put("metricName", prediction.getMetricName());

        return predictionMap;
    }
}
