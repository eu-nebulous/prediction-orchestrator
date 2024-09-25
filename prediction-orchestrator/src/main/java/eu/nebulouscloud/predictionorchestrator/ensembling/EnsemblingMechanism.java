package eu.nebulouscloud.predictionorchestrator.ensembling;

import eu.nebulouscloud.predictionorchestrator.Prediction;
import eu.nebulouscloud.predictionorchestrator.ensembling.ensembler.Ensembler;
import eu.nebulouscloud.predictionorchestrator.ensembling.forecaster_number_verifier.ForecastersNumberVerifier;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@AllArgsConstructor
public class EnsemblingMechanism  {
    public static Prediction POOLED_PREDICTION_NOT_CREATED = null;

    private Ensembler ensembler;
    private ForecastersNumberVerifier forecastersNumberVerifier;

    public Prediction poolPredictions(Map<String, Prediction> predictionsByMethod, String metricName) {
        int expectedForecastersDataCount = predictionsByMethod.size();
        Map<String, Prediction> nonNullPredictions = predictionsByMethod.entrySet()
                .stream()
                .filter(e -> Objects.nonNull(e.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        int numberOfValidData = nonNullPredictions.size();
        if (notEnoughDataToCreatePooledValue(numberOfValidData, expectedForecastersDataCount)) {
            return POOLED_PREDICTION_NOT_CREATED;
        }
        Set<Double> confidence_interval_values = nonNullPredictions.values().stream()
                .map(Prediction::getConfidenceInterval)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        List<Double> confidence_interval = Arrays.asList(
                confidence_interval_values.stream().min(Double::compareTo).get(),
                confidence_interval_values.stream().max(Double::compareTo).get()
        );

        Prediction anyPrediction = nonNullPredictions.values().stream()
                .findAny()
                .get();
        try {

            return new Prediction(
                    ensembler.ensembleValues(predictionsByMethod, metricName),
                    anyPrediction.getComponentId(),
                    System.currentTimeMillis() / 1000,
                    nonNullPredictions.values().stream()
                            .mapToDouble(Prediction::getProbability)
                            .average()
                            .orElse(Double.NaN),
                    confidence_interval,
                    anyPrediction.getPredictionTime(),
                    anyPrediction.getPredictionMethod(),
                    anyPrediction.getMetricName()
            );
        } catch (Exception e) {
            return POOLED_PREDICTION_NOT_CREATED;
        }
    }

    private boolean notEnoughDataToCreatePooledValue(int numberOfValidData, int expectedForecastersDataCount) {
        //We should always pool if we have only one forecaster attached
        if ((numberOfValidData == 1) && (expectedForecastersDataCount == 1)) {
            return false;
        } else {
            return !forecastersNumberVerifier.isDataSufficient(numberOfValidData, expectedForecastersDataCount);
        }
    }
}
