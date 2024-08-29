package eu.nebulouscloud.predictionorchestrator;

import eu.nebulouscloud.predictionorchestrator.ensembling.EnsemblingMechanism;
import eu.nebulouscloud.predictionorchestrator.ensembling.EnsemblingMechanismFactory;
import eu.nebulouscloud.predictionorchestrator.influx.InfluxDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class Orchestrator {

    private final Map<String, LocalDateTime> epochStartMap = new ConcurrentHashMap<>();
    private final Map<String, Integer> timeHorizonMap = new ConcurrentHashMap<>();
    private final Map<String, List<String>> metricNamesMap = new ConcurrentHashMap<>();
    private final TaskScheduler scheduler;

    @Autowired
    private InfluxDBService influxDBService;

    private final EnsemblingMechanism ensemblingMechanism;

    @Autowired
    private PredictionRegistryFactory predictionRegistryFactory;

    private Properties properties;

    @Autowired
    public Orchestrator(InfluxDBService influxDBService, PredictionRegistryFactory predictionRegistryFactory) {

        // Initialize the TaskScheduler
        ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setPoolSize(10);
        taskScheduler.initialize();
        this.scheduler = taskScheduler;

        this.ensemblingMechanism = EnsemblingMechanismFactory.getEnsemblingMechanism(properties);

        this.influxDBService = influxDBService;
        this.predictionRegistryFactory = predictionRegistryFactory;
    }

    public void addApplication(String appName, LocalDateTime epochStart, int timeHorizonMinutes, List<String> metricNames) {
        epochStartMap.put(appName, epochStart);
        timeHorizonMap.put(appName, timeHorizonMinutes);
        metricNamesMap.put(appName, metricNames);
        scheduleTasks(appName);
    }

    private void scheduleTasks(String appName) {
        LocalDateTime epochStart = epochStartMap.get(appName);
        int timeHorizonMinutes = timeHorizonMap.get(appName); // Get the time horizon
        List<String> metricNames = metricNamesMap.get(appName);

        for (int i = 1; i <= properties.getInitial_forward_prediction_number(); i++) {  // Assuming 5 forward predictions, based on the time horizon
            LocalDateTime adaptationTime = epochStart.plusMinutes(i * timeHorizonMinutes); // Apply the time horizon here
            LocalDateTime fetchTime = adaptationTime.minusSeconds(10);  // Fetch data 10 seconds before adaptation

            for (String metricName : metricNames) {
                scheduler.schedule(() -> pullAndEnsemblePredictions(appName, metricName, adaptationTime), fetchTime.atZone(ZoneId.systemDefault()).toInstant());
            }
            scheduler.schedule(() -> performAdaptation(appName, adaptationTime), adaptationTime.atZone(ZoneId.systemDefault()).toInstant());
        }
    }

    private void pullAndEnsemblePredictions(String appName, String metricName, LocalDateTime adaptationTime) {
        LocalDateTime latestFetchTime = adaptationTime.minusSeconds(10);
        LocalDateTime earliestFetchTime = latestFetchTime.minusMinutes(1);

        // Fetch predictions from the database
        Map<String, Prediction> predictionsByMethod = influxDBService.fetchPredictions(appName, metricName, earliestFetchTime, latestFetchTime);

        // Ensemble the predictions using the appropriate mechanism
        Prediction ensembledPrediction = ensemblingMechanism.poolPredictions(predictionsByMethod, metricName);

        // Store the ensembled prediction back
        PredictionRegistry predictionRegistry = predictionRegistryFactory.getRegistryForApplication(appName);
        if (predictionRegistry != null) {
            predictionRegistry.storeEnsembledPrediction(appName,ensembledPrediction);
        }

        PredictedMetricsPublisher predictedMetricsPublisher = new PredictedMetricsPublisher(metricName);
        predictedMetricsPublisher.send(Prediction.toMap(ensembledPrediction));


    }

    private void performAdaptation(String appName, LocalDateTime adaptationTime) {
        PredictionRegistry predictionRegistry = predictionRegistryFactory.getRegistryForApplication(appName);
        if (predictionRegistry != null) {
            // Logic to trigger adaptation based on ensembled predictions
            System.out.println("Performing adaptation for " + appName + " at " + adaptationTime);
            // Use the ensembled data in predictionRegistry for adaptation
        } else {
            System.out.println("No PredictionRegistry found for application: " + appName);
        }
    }
}