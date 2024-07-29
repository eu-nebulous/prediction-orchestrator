package eu.nebulouscloud.predictionorchestrator.messages;

import lombok.Data;

import java.util.List;

@Data
public class StartForecastingMessage {
    private String name;
    private List<String> metrics;
    private long timestamp;
    private long epochStart;
    private int numberOfForwardPredictions;
    private int predictionHorizon;
}