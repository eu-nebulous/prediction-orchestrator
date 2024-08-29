package eu.nebulouscloud.predictionorchestrator;

import eu.nebulouscloud.predictionorchestrator.influx.InfluxDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
@Component
public class PredictionRegistryFactory implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    private final ConcurrentMap<String, PredictionRegistry> registryMap = new ConcurrentHashMap<>();

    @Autowired
    private InfluxDBService influxDBService;

    @Autowired
    private ConfigurableBeanFactory beanFactory;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public PredictionRegistry getRegistryForApplication(String appName) {
        return registryMap.computeIfAbsent(appName, name -> createPredictionRegistry(appName));
    }

    PredictionRegistry createPredictionRegistry(String appName) {
        PredictionRegistry registry = new PredictionRegistry(120, influxDBService); // Use default or config values
        registerPredictionRegistryBean(appName, registry);
        return registry;
    }

    private void registerPredictionRegistryBean(String appName, PredictionRegistry registry) {
        beanFactory.registerSingleton(appName + "PredictionRegistry", registry);
    }
}