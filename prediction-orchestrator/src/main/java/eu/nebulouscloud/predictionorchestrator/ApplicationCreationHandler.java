package eu.nebulouscloud.predictionorchestrator;

import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Handler;
import eu.nebulouscloud.predictionorchestrator.consumers.IntermediateMetricConsumer;
import eu.nebulouscloud.predictionorchestrator.consumers.RealtimeApplicationMetricConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.protonj2.client.Message;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

@Slf4j
public class ApplicationCreationHandler extends Handler {

    @Value("${metric.mode}")
    private String mode;

    @Override
    public void onMessage(String key, String address, Map body, Message message, Context context) {
        try {
            String app_id = message.subject();
            if (app_id == null) app_id = message.property("application").toString();
            log.info("App creation message received {}", app_id);
            if (mode.equals("realtime")) {
                context.registerConsumer(new RealtimeApplicationMetricConsumer(app_id));
            } else if (mode.equals("intermediate")) {
                context.registerConsumer(new IntermediateMetricConsumer(app_id));
            }
        } catch (Exception e) {
            log.error("Error while receiving app creation message", e);
        }
    }
}