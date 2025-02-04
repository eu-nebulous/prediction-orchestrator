package eu.nebulouscloud.predictionorchestrator.communication.publishers;

import eu.nebulouscloud.exn.core.Publisher;

public class StartEnsemblingPublisher extends Publisher {
    public StartEnsemblingPublisher(String publisherKey) {
        super(publisherKey, "eu.nebulouscloud.forecasting.start_ensembling", true, true);
    }
}
