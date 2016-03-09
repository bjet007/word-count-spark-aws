package org.bjean.sample.wordcount.input;

import org.apache.commons.lang3.text.WordUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.commons.lang3.text.WordUtils.wrap;

public class DocumentWriter {

    private final int maxLineLength;

    public DocumentWriter(int maxLineLength) {
        this.maxLineLength = maxLineLength;
    }

    public void write(Path path, String data) throws IOException{
       
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write(wrap(data, maxLineLength));
        }
    }
}
