package eu.nebulouscloud.predictionorchestrator;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class MethodConfig {

    @Value("${methods.names}")
    private String methodsString;

    private static List<String> methods;

    @PostConstruct
    public void init() {
        methods = Arrays.asList(methodsString.split(","));
    }

    public static List<String> getMethodNames() {
        return methods;
    }
}