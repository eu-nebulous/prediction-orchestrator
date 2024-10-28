package eu.nebulouscloud.predictionorchestrator;

import eu.nebulouscloud.exn.core.Publisher;
import eu.nebulouscloud.predictionorchestrator.communication.PublisherFactory;
import eu.nebulouscloud.predictionorchestrator.communication.connectors.BrokerConnectorHandler;
import eu.nebulouscloud.predictionorchestrator.ensembling.EnsemblingMechanism;
import eu.nebulouscloud.predictionorchestrator.ensembling.EnsemblingMechanismFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
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
    private final ConcurrentHashMap<String, Boolean> scheduledTasks = new ConcurrentHashMap<>();

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
        log.info("Orchestrator initialized with a pool size of {} for the TaskScheduler.", properties.getInitial_forward_prediction_number());
    }

    public void addApplication(String appName, LocalDateTime epochStart, List<String> metricNames) {
        if (epochStartMap.containsKey(appName)) {
            log.warn("Application {} is already scheduled. Skipping addition.", appName);
            return;
        }

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

        LocalDateTime now = LocalDateTime.now();
        long epochStartEpoch = epochStart.atZone(ZoneId.systemDefault()).toEpochSecond();
        long nowEpoch = now.atZone(ZoneId.systemDefault()).toEpochSecond();
        long timeHorizonSeconds = timeHorizon;

        long elapsed = nowEpoch - epochStartEpoch;

        long intervalsPassed;
        if (elapsed < 0) {
            intervalsPassed = 0;
            log.debug("Current time is before epochStart for application {}.", appName);
        } else {
            intervalsPassed = (elapsed + timeHorizonSeconds) / timeHorizonSeconds;
            log.debug("Elapsed time since epochStart: {} seconds. Intervals passed: {}.", elapsed, intervalsPassed);
        }

        long nextAdaptationEpoch = epochStartEpoch + (intervalsPassed * timeHorizonSeconds);
        LocalDateTime nextAdaptationTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(nextAdaptationEpoch), ZoneId.systemDefault());

        log.info("Next adaptation time for application {}: {}.", appName, nextAdaptationTime);

        scheduleNextTask(appName, nextAdaptationTime, timeHorizon, metricNames);
    }

    private void scheduleNextTask(String appName, LocalDateTime adaptationEvaluationTime, int timeHorizon, List<String> metricNames) {
        log.info("Scheduling ensembling tasks for application {} at {}.", appName, adaptationEvaluationTime);

        int bufferSeconds = 10;
        int forwardPredictionCount = properties.getInitial_forward_prediction_number();
        log.debug("initial_forward_prediction_number: {}", forwardPredictionCount);

        final LocalDateTime now = LocalDateTime.now();

        if (adaptationEvaluationTime == null) {
            adaptationEvaluationTime = now.plusSeconds(timeHorizon + bufferSeconds);
        }

        for (int i = 0; i < forwardPredictionCount; i++) {
            log.debug("Iteration {} of {} for application {}", i + 1, forwardPredictionCount, appName);
            final LocalDateTime adaptationTime = adaptationEvaluationTime.plusSeconds((i + 1) * timeHorizon);

            Duration durationUntilAdaptation = Duration.between(now, adaptationTime);
            long secondsUntilAdaptation = durationUntilAdaptation.getSeconds();
            log.debug("Seconds until adaptation for iteration {}: {}", i + 1, secondsUntilAdaptation);

            if (secondsUntilAdaptation <= timeHorizon) {
                log.warn("Adaptation time {} for application {} is within the time horizon of {} seconds. Skipping immediate ensembling.", adaptationTime, appName, timeHorizon);
                continue;
            }

            LocalDateTime calculatedEnsembleExecutionTime = adaptationTime.minusSeconds(bufferSeconds + timeHorizon);
            final LocalDateTime ensembleExecutionTime = calculatedEnsembleExecutionTime.isBefore(now.plusSeconds(1)) ? now.plusSeconds(1) : calculatedEnsembleExecutionTime;

            log.debug("Ensembling execution time for application {}: {} (buffer of {} seconds before adaptation evaluation).", appName, ensembleExecutionTime, bufferSeconds);

            for (String metricName : metricNames) {
                String taskKey = appName + ":" + metricName + ":" + adaptationTime;
                if (scheduledTasks.putIfAbsent(taskKey, true) == null) {
                    scheduler.schedule(() -> {
                        try {
                            log.debug("Scheduled ensembling task running for metric {} at {}.", metricName, ensembleExecutionTime);
                            pullAndEnsemblePredictions(appName, metricName, adaptationTime);
                        } finally {
                            scheduledTasks.remove(taskKey);
                        }
                    }, ensembleExecutionTime.atZone(ZoneId.systemDefault()).toInstant());
                }
            }

            final LocalDateTime cleanupTime = adaptationTime.plusSeconds(60);
            log.debug("Scheduling cleanup for application {} at {} (60 seconds after adaptation).", appName, cleanupTime);

            for (String metricName : metricNames) {
                String taskKey = appName + ":cleanup:" + metricName + ":" + adaptationTime;
                if (scheduledTasks.putIfAbsent(taskKey, true) == null) {
                    scheduler.schedule(() -> {
                        try {
                            long adaptationTimeEpoch = adaptationTime.atZone(ZoneId.systemDefault()).toEpochSecond();
                            log.debug("Cleaning up old predictions for application {}, metric {}, at timestamp {}.", appName, metricName, adaptationTimeEpoch);
                            predictionRegistry.cleanupOldPredictions(appName, metricName, adaptationTimeEpoch);
                        } finally {
                            scheduledTasks.remove(taskKey);
                        }
                    }, cleanupTime.atZone(ZoneId.systemDefault()).toInstant());
                }
            }
        }

        final LocalDateTime nextAdaptationEvaluationTime = adaptationEvaluationTime.plusSeconds(timeHorizon);
        log.info("Next adaptation evaluation time for application {}: {}.", appName, nextAdaptationEvaluationTime);

        scheduler.schedule(() -> scheduleNextTask(appName, nextAdaptationEvaluationTime, timeHorizon, metricNames),
                nextAdaptationEvaluationTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    private void pullAndEnsemblePredictions(String appName, String metricName, LocalDateTime adaptationTime) {
        long adaptationTimeEpoch = adaptationTime.atZone(ZoneId.systemDefault()).toEpochSecond();
        log.info("Pulling predictions for application '{}' and metric '{}' at timestamp {}.", appName, metricName, adaptationTimeEpoch);

        if (predictionRegistry != null) {
            Map<String, Prediction> predictionsByMethod = predictionRegistry.getPredictionsByMethodAndTimestamp(appName, metricName, adaptationTimeEpoch);

            if (!predictionsByMethod.isEmpty()) {
                log.debug("Retrieved {} predictions for application '{}' and metric '{}' at timestamp {}.", predictionsByMethod.size(), appName, metricName, adaptationTimeEpoch);

                Prediction ensembledPrediction = ensemblingMechanism.poolPredictions(predictionsByMethod, metricName);
                log.info("Ensembled prediction created for application '{}' and metric '{}' with value: {}", appName, metricName, ensembledPrediction);

                Publisher publisher = publisherFactory.getOrCreatePublisher(metricName);

                if (publisher != null) {
                    try {
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