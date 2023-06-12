package raag.learn.utility;

public enum FilePathConstant {
    FLIGHT_DATA_2015_CSV("Spark/src/main/resources/data/flight-data/csv/2015-summary.csv");
    private final String path;

    private FilePathConstant(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }
}
