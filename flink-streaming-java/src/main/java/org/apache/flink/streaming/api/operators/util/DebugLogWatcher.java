package org.apache.flink.streaming.api.operators.util;

import java.util.Timer;
import java.util.TimerTask;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;

public class DebugLogWatcher{
    Timer timer;
    public DebugLogWatcher(String fileName, int seconds) {
        timer = new Timer();
        timer.schedule(new DebugLogConfigReader(fileName), seconds*1000);
    }

    class DebugLogConfigReader extends TimerTask {
        String fileName;
        int logFrequency;
        DebugLogConfigReader(String fileName) {
            this.fileName = fileName;
            this.logFrequency = -1;
        }

        public void run() {
            File f = new File(this.fileName);
            if (f.exists()) {
                try {
                    FileReader fr = new FileReader(f);
                    BufferedReader reader = new BufferedReader(fr);
                    String line = reader.readLine();
                    reader.close();
                    if (line != null) {
                        try {
                            this.logFrequency = Integer.parseInt(line);
                        } catch (NumberFormatException ex){
                        }
                    }
                } catch (FileNotFoundException ex) {

                } catch (IOException ex) {

                }
            } else {
                this.logFrequency = -1;
            }
        }
    }
}
