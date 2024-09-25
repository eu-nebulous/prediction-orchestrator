package eu.nebulouscloud.predictionorchestrator.communication.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NonNull;

import java.util.Map;

@AllArgsConstructor
public class PredictionsToEnsembleMessage {

    @JsonProperty("method")
    @NonNull
    private String ensemblingMethod;

    @JsonProperty("metric")
    @NonNull
    private String metric;

    @JsonProperty("predictionTime")
    @NonNull
    private Long predictionTime;

    @JsonProperty("predictionsToEnsemble")
    @NonNull
    private Map<String, Double> predictionsByForecaster;
}
