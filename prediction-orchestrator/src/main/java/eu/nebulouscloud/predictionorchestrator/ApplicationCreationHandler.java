package eu.nebulouscloud.predictionorchestrator;

import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Handler;
import eu.nebulouscloud.predictionorchestrator.communication.consumers.IntermediateMetricsConsumer;
import eu.nebulouscloud.predictionorchestrator.communication.consumers.RealtimeApplicationMetricConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.protonj2.client.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
public class ApplicationCreationHandler extends Handler {

    @Value("${metric.mode}")
    private String mode = "intermediate";

    private final PredictionRegistryFactory predictionRegistryFactory;

    @Autowired
    public ApplicationCreationHandler(PredictionRegistryFactory predictionRegistryFactory) {
        this.predictionRegistryFactory = predictionRegistryFactory;
    }

    @Override
    public void onMessage(String key, String address, Map body, Message message, Context context) {
        try {
            String app_id = message.subject();
            if (app_id == null) {
                app_id = message.property("application").toString();
            }
            log.info("App creation message received for application: {}", app_id);

            if (mode.equals("realtime")) {
                context.registerConsumer(new RealtimeApplicationMetricConsumer(app_id));
            } else if (mode.equals("intermediate")) {
                PredictionRegistry predictionRegistry = predictionRegistryFactory.getRegistryForApplication(app_id);
                context.registerConsumer(new IntermediateMetricsConsumer(app_id, predictionRegistry));
            }
        } catch (Exception e) {
            log.error("Error while receiving app creation message", e);
        }
    }
}