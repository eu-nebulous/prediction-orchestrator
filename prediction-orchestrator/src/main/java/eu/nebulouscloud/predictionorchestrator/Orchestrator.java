package eu.nebulouscloud.predictionorchestrator;

import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Publisher;
import eu.nebulouscloud.predictionorchestrator.communication.PublisherFactory;
import eu.nebulouscloud.predictionorchestrator.communication.connectors.BrokerConnectorHandler;
import eu.nebulouscloud.predictionorchestrator.communication.publishers.PredictedMetricsPublisher;
import eu.nebulouscloud.predictionorchestrator.ensembling.EnsemblingMechanism;
import eu.nebulouscloud.predictionorchestrator.ensembling.EnsemblingMechanismFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class Orchestrator {

    private final Map<String, LocalDateTime> epochStartMap = new ConcurrentHashMap<>();
    private final Map<String, Integer> timeHorizonMap = new ConcurrentHashMap<>();
    private final Map<String, List<String>> metricNamesMap = new ConcurrentHashMap<>();
    private final TaskScheduler scheduler;
    private final EnsemblingMechanism ensemblingMechanism;
    private final Properties properties;
    private final PredictionRegistry predictionRegistry;

    @Autowired
    private PublisherFactory publisherFactory;

    @Autowired
    public Orchestrator(Properties properties, PredictionRegistry predictionRegistry, BrokerConnectorHandler brokerConnectorHandler) {
        ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setPoolSize(properties.getInitial_forward_prediction_number());
        taskScheduler.initialize();
        this.scheduler = taskScheduler;

        this.properties = properties;
        this.predictionRegistry = predictionRegistry;
        this.ensemblingMechanism = EnsemblingMechanismFactory.getEnsemblingMechanism(properties);
        log.info("Orchestrator initialized with a pool size of 10 for the TaskScheduler.");
    }

    public void addApplication(String appName, LocalDateTime epochStart, List<String> metricNames) {
        log.info("Adding application {} with epoch start {} and time horizon {} seconds.", appName, epochStart, properties.getInitial_prediction_horizon());

        epochStartMap.put(appName, epochStart);
        timeHorizonMap.put(appName, properties.getInitial_prediction_horizon());
        metricNamesMap.put(appName, metricNames);

        scheduleTasks(appName);
    }

    private void scheduleTasks(String appName) {
        log.info("Scheduling tasks for application {}.", appName);

        LocalDateTime epochStart = epochStartMap.get(appName);
        int timeHorizon = timeHorizonMap.get(appName);
        List<String> metricNames = metricNamesMap.get(appName);

        // Start scheduling the next task based on epochStart
        scheduleNextTask(appName, epochStart, timeHorizon, metricNames);
    }

    private void scheduleNextTask(String appName, LocalDateTime adaptationTime, int timeHorizon, List<String> metricNames) {
        log.info("Scheduling next task for application {} at {}.", appName, adaptationTime);

        // Calculate fetch time
        LocalDateTime fetchTime = adaptationTime.minusSeconds(10);
        log.debug("Fetch time for application {}: {} (10 seconds before adaptation).", appName, fetchTime);

        long adaptationTimeEpoch = adaptationTime.atZone(ZoneId.systemDefault()).toEpochSecond();
        log.debug("Epoch time for application {}: {}", appName, adaptationTimeEpoch);

        // Schedule ensembling for each metric
        for (String metricName : metricNames) {
            scheduler.schedule(() -> {
                log.debug("Scheduled task running for metric {} at time {}.", metricName, adaptationTime);
                pullAndEnsemblePredictions(appName, metricName, adaptationTime);
            }, fetchTime.atZone(ZoneId.systemDefault()).toInstant());
        }

        // Schedule cleanup of old predictions
        LocalDateTime cleanupTime = adaptationTime.plusSeconds(60);
        log.debug("Scheduling cleanup for application {} at {} (1 minute after adaptation).", appName, cleanupTime);

        for (String metricName : metricNames) {
            scheduler.schedule(() -> {
                log.debug("Cleaning up old predictions for application {}, metric {}, at timestamp {}.", appName, metricName, adaptationTimeEpoch);
                predictionRegistry.cleanupOldPredictions(appName, metricName, adaptationTimeEpoch);
            }, cleanupTime.atZone(ZoneId.systemDefault()).toInstant());
        }

        // Calculate next adaptation time and recursively schedule the next task
        LocalDateTime nextAdaptationTime = adaptationTime.plusSeconds(timeHorizon);
        log.info("Next adaptation time for application {}: {}.", appName, nextAdaptationTime);

        scheduler.schedule(() -> scheduleNextTask(appName, nextAdaptationTime, timeHorizon, metricNames),
                nextAdaptationTime.atZone(ZoneId.systemDefault()).toInstant());
    }


    private void pullAndEnsemblePredictions(String appName, String metricName, LocalDateTime adaptationTime) {
        long adaptationTimeEpoch = adaptationTime.atZone(ZoneId.systemDefault()).toEpochSecond();
        log.info("Pulling predictions for application '{}' and metric '{}' at timestamp {}.", appName, metricName, adaptationTimeEpoch);

        if (predictionRegistry != null) {
            Map<String, Prediction> predictionsByMethod = predictionRegistry.getPredictionsByMethodAndTimestamp(appName, metricName, adaptationTimeEpoch);

            if (!predictionsByMethod.isEmpty()) {
                log.debug("Retrieved {} predictions for application '{}' and metric '{}' at timestamp {}.", predictionsByMethod.size(), appName, metricName, adaptationTimeEpoch);

                // Perform ensembling
                Prediction ensembledPrediction = ensemblingMechanism.poolPredictions(predictionsByMethod, metricName, appName);
                log.info("Ensembled prediction created for application '{}' and metric '{}' with value: {}", appName, metricName, ensembledPrediction);

                // Retrieve the Publisher via PublisherFactory
                Publisher publisher = publisherFactory.getOrCreatePublisher(metricName);

                if (publisher != null) {
                    try {
                        // Send the ensembled prediction for publishing
                        publisher.send(Prediction.toMap(ensembledPrediction), appName);
                        log.info("Ensembled prediction for application '{}' and metric '{}' sent to publisher successfully.", appName, metricName);
                    } catch (Exception e) {
                        log.error("Failed to send ensembled prediction for application '{}' and metric '{}'. Exception: {}", appName, metricName, e.getMessage(), e);
                    }
                } else {
                    log.error("Unable to obtain Publisher for metric '{}' and application '{}'.", metricName, appName);
                }

            } else {
                log.warn("No predictions found for application '{}' and metric '{}' at timestamp {}.", appName, metricName, adaptationTimeEpoch);
            }
        } else {
            log.error("PredictionRegistry is null. Cannot pull predictions for application '{}' and metric '{}'.", appName, metricName);
        }
    }
}