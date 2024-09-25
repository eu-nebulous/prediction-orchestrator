package eu.nebulouscloud.predictionorchestrator.communication.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NonNull;
import lombok.ToString;

import javax.validation.constraints.Min;

@Data
@ToString
public class MetricNeedingPredictingMessage {

    @NonNull
    private String metric;

    @NonNull
    @Min(0)
    private String level;

    @NonNull
    @Min(1)
    private int publish_rate;

    @JsonProperty("version")
    @NonNull
    private int version;
}

