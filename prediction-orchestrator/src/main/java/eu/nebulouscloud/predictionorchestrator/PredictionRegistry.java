package eu.nebulouscloud.predictionorchestrator;

import eu.nebulouscloud.predictionorchestrator.influx.InfluxDBService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
public class PredictionRegistry {
    private final long predictionHorizon;
    private final AtomicReference<Long> epochStart = new AtomicReference<>(null);
    private final CountDownLatch epochStartLatch = new CountDownLatch(1);
    private final InfluxDBService influxDBService;

    private final Map<String, Map<String, Prediction>> storedPredictions = new ConcurrentHashMap<>(); // Stores ensembled predictions by metric name

    public PredictionRegistry(
            @Value("${prediction.horizon}") long predictionHorizon,
            InfluxDBService influxDBService) {
        this.predictionHorizon = predictionHorizon;
        this.influxDBService = influxDBService;
    }

    public void setEpochStart(long epochStart) {
        this.epochStart.set(epochStart);
        epochStartLatch.countDown();  // Signal that the epoch start has been set
    }

    public void storeEnsembledPrediction(String applicationName, Prediction ensembledPrediction) {
        try {
            String bucketName = "ensembledPredictions_" + applicationName;

            // Write the ensembled prediction to InfluxDB under the specified bucket
            influxDBService.writePrediction(bucketName, ensembledPrediction);

            log.info("Successfully stored ensembled prediction for application {} in bucket {}", applicationName, bucketName);
        } catch (Exception e) {
            log.error("Failed to store ensembled prediction for application {}", applicationName, e);
        }
    }

    private boolean isValidPredictionTime(long predictionTime, long epochStart) {
        return predictionTime >= epochStart && (predictionTime - epochStart) % predictionHorizon == 0;
    }

    private boolean hasSufficientHorizon(Prediction prediction) {
        long timeDelta = prediction.getPredictionTime() - prediction.getTimestamp();
        return timeDelta >= predictionHorizon;
    }

    public void storePrediction(String applicationName, Prediction prediction) {
        try {
            epochStartLatch.await();  // Wait until epochStart is set

            long epochStartValue = epochStart.get();
            if (!isValidPredictionTime(prediction.getPredictionTime(), epochStartValue)) {
                log.info("Prediction for application {} and metric {} at time {} is disregarded due to invalid time point (epoch start: {}, prediction horizon: {})",
                        applicationName, prediction.getMetricName(), prediction.getPredictionTime(), epochStartValue, predictionHorizon);
                return;
            }

            if (!hasSufficientHorizon(prediction)) {
                long timeDelta = prediction.getPredictionTime() - prediction.getTimestamp();
                log.info("Prediction for application {} and metric {} at time {} is disregarded due to insufficient horizon ({} seconds)",
                        applicationName, prediction.getMetricName(), prediction.getPredictionTime(), timeDelta);
                return;
            }

            // Store the prediction in the map
            storedPredictions
                    .computeIfAbsent(applicationName, k -> new ConcurrentHashMap<>())
                    .put(prediction.getMetricName(), prediction);

            // Optionally, write the prediction to InfluxDB
            influxDBService.writePrediction(applicationName, prediction);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while waiting for epoch start to be set", e);
        }
    }

    public Prediction getPrediction(String applicationName, String metricName) {
        Map<String, Prediction> predictionsByMetric = storedPredictions.get(applicationName);
        if (predictionsByMetric != null) {
            return predictionsByMetric.get(metricName);
        }
        return null;
    }

}