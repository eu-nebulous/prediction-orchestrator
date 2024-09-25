package eu.nebulouscloud.predictionorchestrator.communication.messages;


import lombok.Data;
import org.json.JSONObject;

@Data
public class Metric {
    private String name;
    private String upperBound;
    private String lowerBound;

    public Metric(String name, String upperBound, String lowerBound) {
        this.name = name;
        this.upperBound = upperBound;
        this.lowerBound = lowerBound;
    }

    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("name", name);
        json.put("upper_bound", upperBound);
        json.put("lower_bound", lowerBound);
        return json;
    }
}