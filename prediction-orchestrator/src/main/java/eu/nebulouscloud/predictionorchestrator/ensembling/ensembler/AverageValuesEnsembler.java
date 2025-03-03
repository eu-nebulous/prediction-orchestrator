package eu.nebulouscloud.predictionorchestrator.ensembling.ensembler;


import eu.nebulouscloud.predictionorchestrator.Prediction;

import java.util.Map;
import java.util.Objects;

public class AverageValuesEnsembler extends Ensembler {


    @Override
    public double ensembleValues(Map<String, Prediction> predictionsByMethod, String metricName, String appName) {
        return predictionsByMethod.values().stream()
                .filter(Objects::nonNull)
                .mapToDouble(Prediction::getMetricValue)
                .average()
                .orElse(Double.NaN);
    }
}
