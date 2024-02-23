package eu.nebulouscloud.predictionorchestrator;

import eu.nebulouscloud.exn.core.Consumer;
import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Handler;
import eu.nebulouscloud.exn.handlers.ConnectorHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.protonj2.client.Message;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
class PredictionOrchestratorHandler extends ConnectorHandler {

    @Override
    public void onReady(Context context) {
        log.info("Prediction Orchestrator is ready");

    }

}
