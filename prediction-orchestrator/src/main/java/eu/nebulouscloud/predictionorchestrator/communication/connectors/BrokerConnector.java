package eu.nebulouscloud.predictionorchestrator.communication.connectors;

import eu.nebulouscloud.exn.Connector;
import eu.nebulouscloud.exn.core.Consumer;
import eu.nebulouscloud.exn.settings.StaticExnConfig;
import eu.nebulouscloud.predictionorchestrator.Orchestrator;
import eu.nebulouscloud.predictionorchestrator.Properties;
import eu.nebulouscloud.predictionorchestrator.communication.consumers.MetricsListConsumer;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class BrokerConnector {

    @Value("${exn.host}")
    private String host;

    @Value("${exn.port}")
    private int port;

    @Value("${exn.username}")
    private String username;

    @Value("${exn.password}")
    private String password;

    @Value("${exn.retryAttempts}")
    private int retryAttempts;

    public static final String app_creation_channel = "eu.nebulouscloud.ui.dsl.generic";

    @Autowired
    private ApplicationCreationHandler applicationCreationHandler;

    @Autowired
    private Orchestrator orchestrator;

    @Autowired
    private Properties properties;

    @Autowired
    private BrokerConnectorHandler brokerConnectorHandler;

    @PostConstruct
    public void init() {
        startConnector();
    }

    private void startConnector() {
        try {

            Connector connector = new Connector(
                    "prediction_orchestrator",
                    brokerConnectorHandler,
                    List.of(), // List of publishers
                    List.of(new Consumer("ui_app_messages", app_creation_channel,
                            applicationCreationHandler, true, true), new MetricsListConsumer(orchestrator, properties)),
                    true, // enableState
                    true, // enableHealth
                    new StaticExnConfig(host, port, username, password, retryAttempts) // Configuration
            );
            // Start the connector
            try {
                connector.start();
                log.info("Connector started successfully.");
            } catch (Exception e) {
                log.error("Failed to start the connector", e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}