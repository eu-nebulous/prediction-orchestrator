package eu.nebulouscloud.predictionorchestrator.ensembling.ensembler;


import eu.nebulouscloud.predictionorchestrator.Prediction;

import java.util.Map;

public abstract class Ensembler {

    public abstract double ensembleValues(Map<String, Prediction> predictionsByMethod, String metricName, String appName);

}
