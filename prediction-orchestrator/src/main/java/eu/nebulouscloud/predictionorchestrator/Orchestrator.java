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

    /**
     * Called externally to register a new application (with an epochStart time, a list
     * of metric names, and the default horizon from properties). Then we schedule tasks.
     */
    public void addApplication(String appName, LocalDateTime epochStart, List<String> metricNames) {
        log.info("Adding application {} with epoch start {} and time horizon {} seconds.",
                appName, epochStart, properties.getInitial_prediction_horizon());

        epochStartMap.put(appName, epochStart);
        timeHorizonMap.put(appName, properties.getInitial_prediction_horizon());
        metricNamesMap.put(appName, metricNames);

        scheduleTasks(appName);
    }

    /**
     * Begin the scheduling loop for a newly added application.
     */
    private void scheduleTasks(String appName) {
        log.info("Scheduling tasks for application {}.", appName);

        LocalDateTime epochStart = epochStartMap.get(appName);
        int timeHorizon = timeHorizonMap.get(appName);
        List<String> metricNames = metricNamesMap.get(appName);

        // Start scheduling the next task.
        // Use 'epochStart' as the initial "adaptationTime" if you like, or LocalDateTime.now().
        scheduleNextTask(appName, epochStart, timeHorizon, metricNames);
    }

    /**
     * The core scheduling logic. We:
     * 1. Determine the current time (t1).
     * 2. Find the nearest horizon p1 = epochStart + N*horizon (where N is the next integer).
     * 3. If p1 - t1 < horizon => skip p1 => p2 = p1 + horizon.
     * 4. Schedule the pull at (chosenHorizon - horizon - 10).
     * 5. Optionally schedule cleanup, then schedule next iteration.
     */
    private void scheduleNextTask(String appName,
                                  LocalDateTime adaptationTime,
                                  int timeHorizon,
                                  List<String> metricNames) {

        log.info("Scheduling next task for application {} at adaptation time {}.", appName, adaptationTime);

        // current time (t1):
        long currentTimeSeconds = System.currentTimeMillis() / 1000;

        // Convert the stored epochStart into seconds:
        long epochStartSeconds = epochStartMap.get(appName)
                .atZone(ZoneId.systemDefault())
                .toEpochSecond();

        // Calculate the number of full horizons since epoch start
        long horizonCountSinceEpoch = (currentTimeSeconds - epochStartSeconds) / timeHorizon;
        log.info("For application '{}', epoch start (seconds): {} and horizon count since epoch: {}.",
                appName, epochStartSeconds, horizonCountSinceEpoch);

        // Step 1: find the *nearest* horizon p1 that is >= t1
        //   p1 = epochStart + (⌊(t1 - epochStart)/horizon⌋ + 1)* horizon
        long candidateHorizonTime = epochStartSeconds + ((horizonCountSinceEpoch + 1) * timeHorizon);

        long diff = candidateHorizonTime - currentTimeSeconds;

        // Step 2: If p1 - t1 < horizon => skip p1 => use p2
        if (diff < timeHorizon) {
            // i.e. we're "too close" to p1, so let's jump to p2
            log.info("p1 - t1 = {} seconds (< {}), skipping horizon {} -> using {}",
                    diff, timeHorizon, candidateHorizonTime, (candidateHorizonTime + timeHorizon));
            candidateHorizonTime += timeHorizon;
        } else {
            log.info("p1 - t1 = {} seconds (>= {}), we use nearest horizon {}",
                    diff, timeHorizon, candidateHorizonTime);
        }

        // Store the final horizon in a "final" variable for use in lambdas
        final long chosenHorizonTime = candidateHorizonTime;

        // Step 3: We schedule the fetch at (p - horizon - 10),
        //         i.e. p - (timeHorizon + 10)
        final long fetchTimeEpoch = chosenHorizonTime - (timeHorizon + 10);

        // Convert to LocalDateTime
        final LocalDateTime fetchTime = LocalDateTime.ofEpochSecond(
                fetchTimeEpoch,
                0,
                ZoneId.systemDefault().getRules().getOffset(LocalDateTime.now())
        );

        log.info("Will fetch predictions for horizon {} at {} (which is horizon - {} - 10).",
                chosenHorizonTime, fetchTime, timeHorizon);

        // Step 4: schedule the "pull" step for each metric at 'fetchTime'
        for (String metricName : metricNames) {
            scheduler.schedule(
                    () -> pullAndEnsemblePredictions(appName, metricName, chosenHorizonTime),
                    fetchTime.atZone(ZoneId.systemDefault()).toInstant()
            );
        }

        long adaptationTimeSeconds = adaptationTime
                .atZone(ZoneId.systemDefault())
                .toEpochSecond();
        final LocalDateTime cleanupTime = adaptationTime.plusSeconds(60);

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

        // Step 5: schedule next adaptation
        final LocalDateTime nextAdaptationTime = adaptationTime.plusSeconds(timeHorizon);
        log.info("Next adaptation time for application {}: {}.", appName, nextAdaptationTime);

        scheduler.schedule(
                () -> scheduleNextTask(appName, nextAdaptationTime, timeHorizon, metricNames),
                nextAdaptationTime.atZone(ZoneId.systemDefault()).toInstant()
        );
    }

    /**
     * Actually performs the "pull" from PredictionRegistry and the ensembling,
     * then publishes it.
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

                // Ensembling
                Prediction ensembledPrediction =
                        ensemblingMechanism.poolPredictions(predictionsByMethod, metricName, appName);
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
