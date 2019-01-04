package Config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;


public class Config {

    public static final String filePath = "./src/main/java/config.json";

    public static Config loadConfig() throws IOException {

        Gson gson = new GsonBuilder().create();

        Path path = new File(filePath ).toPath();

        Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);

        return gson.fromJson(reader,  Config.class);
    }

    public static Config defaultConfig(){
        return new Config(true, 1,1,5000,12345);
    }


    private final boolean debugMode;
    private final int numCoordinators;
    private final int numWorkers;
    private final int timeout;
    private final int defaultPort;



    public Config(boolean debugMode, int numCoordinators, int numWorkers, int timeout, int defaultPort) {
        this.debugMode = debugMode;
        this.numCoordinators = numCoordinators;
        this.numWorkers = numWorkers;
        this.timeout = timeout;
        this.defaultPort = defaultPort;
    }

    public boolean getDebugMode() {
        return debugMode;
    }

    public int getDefaultPort(){
        return this.defaultPort;
    }
    public int getNumCoordinators() {
        return numCoordinators;
    }

    public int getNumWorkers() {
        return numWorkers;
    }

    public int getTimeout() {
        return timeout;
    }

    public boolean getDebugModeDefault() {
        return true;
    }

    public int getNumCoordinatorsDefault() {
        return 1;
    }

    public int getNumWorkersDefault() {
        return 1;
    }

    public int getTimeoutDefault() {
        return 50000;
    }

    public static String getFilePath() {
        return filePath;
    }

    public String toString(){

        StringBuilder sb = new StringBuilder();
        sb.append( "Debug = ").append(debugMode).append("\n");
        sb.append( "NumCoordinators = ").append(numCoordinators).append("\n");
        sb.append( "NumWorkers = ").append(numWorkers).append("\n");
        sb.append( "timeout = ").append(timeout).append("\n");
        return sb.toString();
    }

}
