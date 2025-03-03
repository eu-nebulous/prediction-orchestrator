package eu.nebulouscloud.predictionorchestrator.ensembling.ensembler;

import eu.nebulouscloud.predictionorchestrator.Prediction;
import eu.nebulouscloud.predictionorchestrator.communication.messages.PredictionsEnsembledMessage;
import eu.nebulouscloud.predictionorchestrator.communication.messages.PredictionsToEnsembleMessage;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@Slf4j
public class OuterEnsembler extends Ensembler {

    private EnsemblerService ensemblerService;

    @Override
    public double ensembleValues(Map<String, Prediction> predictionsByMethod, String metricName, String appName) {
        long predictionTime =  predictionsByMethod.values().stream()
                .findFirst()
                .get()
                .getPredictionTime();
        PredictionsToEnsembleMessage predictionsToEnsembleMessage = new PredictionsToEnsembleMessage(
                "BestSubset",
                metricName,
                predictionTime,
                predictionsByMethod.entrySet().stream()
                        .collect(HashMap::new,
                                (m, v) -> m.put(v.getKey(), Double.valueOf(v.getValue() == null ? null : v.getValue().getMetricValue())),
                                HashMap::putAll),
                appName

        );
        PredictionsEnsembledMessage ensembledMessage;
        try {
            ensembledMessage = ensemblerService.ensemble(predictionsToEnsembleMessage);
        } catch (Exception e) {
            log.info("Error when sending ensembling request: {}", e.getMessage());
            throw new IllegalArgumentException("Error while sending message to the Ensember");
        }

        if (ensembledMessage == null) {
            log.warn("Message from ensembler is null");
            throw new IllegalArgumentException("Message from the Ensembler is null");
        }
        if (ensembledMessage.getPredictionTime() != predictionTime) {
            log.warn("Message from the Ensembler has wrong predictionTime");
            throw new IllegalArgumentException("Message from the Ensembler has wrong predictionTime");
        }
        return ensembledMessage.getEnsembledValue();
    }
}
