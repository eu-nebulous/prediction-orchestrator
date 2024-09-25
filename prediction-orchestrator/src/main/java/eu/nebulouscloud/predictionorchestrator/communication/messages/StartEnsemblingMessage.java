package eu.nebulouscloud.predictionorchestrator.communication.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.NonNull;

import java.util.List;

@AllArgsConstructor
public class StartEnsemblingMessage {

    @JsonProperty("metrics")
    @NonNull
    private List<MetricNeedingPredictingMessage> metrics;

    @JsonProperty("models")
    @NonNull
    private List<String> methodNames;
}
