package eu.nebulouscloud.predictionorchestrator.communication.connectors;

import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Handler;
import eu.nebulouscloud.predictionorchestrator.PredictionRegistry;
import eu.nebulouscloud.predictionorchestrator.communication.consumers.IntermediateMetricsConsumer;
import eu.nebulouscloud.predictionorchestrator.communication.consumers.RealtimeApplicationMetricConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.protonj2.client.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
public class ApplicationCreationHandler extends Handler {

    @Value("${metric.mode}")
    private String mode;

    @Autowired
    private PredictionRegistry predictionRegistry;

    @Override
    public void onMessage(String key, String address, Map body, Message message, Context context) {
        try {
            String app_id = Optional.ofNullable(message.subject())
                    .orElse(message.property("application").toString());

            log.info("App creation message received for application: {}", app_id);

            // Switch based on mode (cleaner than if-else)
            switch (mode) {
                case "realtime":
                    context.registerConsumer(new RealtimeApplicationMetricConsumer(app_id));
                    break;
                case "intermediate":
                    break;
                default:
                    log.warn("Unknown mode: {}", mode);
            }

        } catch (Exception e) {
        	log.error("Failed processing key:{}, address:{}, body:{}",key,address,body,e );
        }
    }
}