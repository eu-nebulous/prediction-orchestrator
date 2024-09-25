package eu.nebulouscloud.predictionorchestrator.communication.connectors;

import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.handlers.ConnectorHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Getter
@Slf4j
@Component
public class BrokerConnectorHandler extends ConnectorHandler {

    private Context context;

    @Override
    public void onReady(Context context) {
        log.info("BrokerConnectorHandler: Broker connector is registered");
        this.context = context;
    }
}