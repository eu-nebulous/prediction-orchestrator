package eu.nebulouscloud.predictionorchestrator;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Prediction {

    private float metricValue;
    private int level;
    private long timestamp;
    private float probability;
    private float[] confidenceInterval;
    private long predictionTime;
    private String componentId;
}
