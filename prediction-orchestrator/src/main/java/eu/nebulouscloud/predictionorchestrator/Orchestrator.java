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
    public Orchestrator(Properties properties,
                        PredictionRegistry predictionRegistry,
                        BrokerConnectorHandler brokerConnectorHandler) {

        ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setPoolSize(properties.getInitial_forward_prediction_number());
        taskScheduler.initialize();
        this.scheduler = taskScheduler;

        this.properties = properties;
        this.predictionRegistry = predictionRegistry;
        this.ensemblingMechanism = EnsemblingMechanismFactory.getEnsemblingMechanism(properties);

        log.info("Orchestrator initialized with a pool size of {} for the TaskScheduler.",
                properties.getInitial_forward_prediction_number());
    }

    public void addApplication(String appName, LocalDateTime epochStart, List<String> metricNames) {
        log.info("Adding application {} with epoch start {} and time horizon {} seconds.",
                appName, epochStart, properties.getInitial_prediction_horizon());

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

    private void scheduleNextTask(String appName,
                                  LocalDateTime adaptationTime,
                                  int timeHorizon,
                                  List<String> metricNames) {

        log.info("Scheduling next task for application {} at adaptation time {}.", appName, adaptationTime);

        long epochStartSeconds = epochStartMap.get(appName)
                .atZone(ZoneId.systemDefault())
                .toEpochSecond();
        long adaptationTimeSeconds = adaptationTime
                .atZone(ZoneId.systemDefault())
                .toEpochSecond();

        // Compute the "next horizon" time in seconds
        long nextPredictionTime = epochStartSeconds
                + (((adaptationTimeSeconds - epochStartSeconds) / timeHorizon) + 1) * timeHorizon;

        // ***** FIX: We want to fetch 10s AFTER this horizon (1 interval ahead + 10s) *****
        long fetchTimeEpoch = nextPredictionTime + 10;
        LocalDateTime fetchTime = LocalDateTime.ofEpochSecond(
                fetchTimeEpoch, 0,
                ZoneId.systemDefault().getRules().getOffset(adaptationTime)
        );

        log.info("Fetch time for application {}: {} (10 seconds AFTER horizon at {}).",
                appName, fetchTime, nextPredictionTime);

        // Schedule the "pull" step at horizon + 10s
        for (String metricName : metricNames) {
            scheduler.schedule(
                    () -> pullAndEnsemblePredictions(appName, metricName, nextPredictionTime),
                    fetchTime.atZone(ZoneId.systemDefault()).toInstant()
            );
        }

        // (Optional) schedule cleanup of old predictions
        LocalDateTime cleanupTime = adaptationTime.plusSeconds(60);
        log.debug("Scheduling cleanup for application {} at {} (1 minute after adaptation).",
                appName, cleanupTime);

        for (String metricName : metricNames) {
            scheduler.schedule(
                    () -> {
                        log.debug("Cleaning up old predictions for app {}, metric {}, at timestamp {}.",
                                appName, metricName, adaptationTimeSeconds);
                        predictionRegistry.cleanupOldPredictions(appName, metricName, adaptationTimeSeconds);
                    },
                    cleanupTime.atZone(ZoneId.systemDefault()).toInstant()
            );
        }

        // Schedule the next adaptation
        LocalDateTime nextAdaptationTime = adaptationTime.plusSeconds(timeHorizon);
        log.info("Next adaptation time for application {}: {}.", appName, nextAdaptationTime);

        scheduler.schedule(
                () -> scheduleNextTask(appName, nextAdaptationTime, timeHorizon, metricNames),
                nextAdaptationTime.atZone(ZoneId.systemDefault()).toInstant()
        );
    }

    /**
     * Pull all predictions for the given (appName, metricName) at the specified horizon time,
     * then ensemble them into a single prediction, and publish it.
     */
    private void pullAndEnsemblePredictions(String appName, String metricName, long predictionTime) {
        long currentTimestamp = System.currentTimeMillis() / 1000;
        long epochStart = epochStartMap.get(appName)
                .atZone(ZoneId.systemDefault())
                .toEpochSecond();

        log.info("Pulling predictions for application '{}' and metric '{}' at current timestamp {}. "
                        + "Epoch start: {}, Prediction timestamp: {}.",
                appName, metricName, currentTimestamp, epochStart, predictionTime);

        if (predictionRegistry != null) {
            Map<String, Prediction> predictionsByMethod =
                    predictionRegistry.getPredictionsByMethodAndTimestamp(
                            appName, metricName, predictionTime
                    );

            if (!predictionsByMethod.isEmpty()) {
                log.debug("Retrieved {} predictions for '{}'/ '{}' at timestamp {}.",
                        predictionsByMethod.size(), appName, metricName, predictionTime);

                // Perform ensembling
                Prediction ensembledPrediction = ensemblingMechanism.poolPredictions(
                        predictionsByMethod, metricName, appName
                );
                log.info("Ensembled prediction created for '{}'/ '{}': {}",
                        appName, metricName, ensembledPrediction);

                // Publish
                Publisher publisher = publisherFactory.getOrCreatePublisher(metricName);
                if (publisher != null) {
                    try {
                        publisher.send(Prediction.toMap(ensembledPrediction), appName);
                        log.info("Ensembled prediction for '{}'/ '{}' sent to publisher successfully.",
                                appName, metricName);
                    } catch (Exception e) {
                        log.error("Failed to send ensembled prediction for '{}'/ '{}'. Exception: {}",
                                appName, metricName, e.getMessage(), e);
                    }
                } else {
                    log.error("Unable to obtain Publisher for metric '{}' in application '{}'.",
                            metricName, appName);
                }
            } else {
                log.warn("No predictions found for '{}'/ '{}' at timestamp {}.",
                        appName, metricName, predictionTime);
            }
        } else {
            log.error("PredictionRegistry is null. Cannot pull predictions for '{}'/ '{}'.",
                    appName, metricName);
        }
    }
}
