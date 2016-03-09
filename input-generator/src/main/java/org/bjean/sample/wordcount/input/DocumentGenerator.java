package org.bjean.sample.wordcount.input;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.StringJoiner;

public class DocumentGenerator {

    private final Random random = new Random();
    private final DocumentWriter documentWriter = new DocumentWriter(80);
    private final RandomWordGenerator wordGenerator = new RandomWordGenerator(random);
    public static void main(String[] args) {
      
        
    }

    

    
}
