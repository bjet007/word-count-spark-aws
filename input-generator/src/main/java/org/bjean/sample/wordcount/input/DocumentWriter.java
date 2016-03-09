package org.bjean.sample.wordcount.input;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DocumentWriter {
    
    public void write(Path path, String data) throws IOException{
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write(data);
        }
    }
}
