package eu.nebulouscloud.predictionorchestrator.ensembling.ensembler;

public enum EnsemblerType {
    AVERAGE("average"),

    OUTER("outer");

    private String type;

    EnsemblerType(String type) {
        this.type = type;
    }
}
