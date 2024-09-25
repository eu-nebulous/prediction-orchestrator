package eu.nebulouscloud.predictionorchestrator.communication.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class PredictionsEnsembledMessage {

    @JsonProperty("ensembledValue")
    @NonNull
    private double ensembledValue;

    @JsonProperty("timestamp")
    @NonNull
    private long timestamp;

    @JsonProperty("predictionTime")
    @NonNull
    private long predictionTime;

}
