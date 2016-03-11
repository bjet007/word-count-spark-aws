package org.bjean.sample.wordcount.input;

import com.vtence.cli.CLI;
import com.vtence.cli.args.Args;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.StringJoiner;

public class DocumentGenerator {

    private final Random random = new Random();
    private final DocumentWriter documentWriter = new DocumentWriter(80);
    private final RandomWordGenerator wordGenerator = new RandomWordGenerator(random);
    private File outputPath;
    private int numberOfParagraph;

    public static void main(String... args) throws IOException {

        Args extractedArgument = parseArguments(args);
        new DocumentGenerator(extractedArgument.get("out"), extractedArgument.get("--number")).generate();
    }

    private static Args parseArguments(String... args) throws IOException {
        CLI cli = new CLI() {{
            name("DocumentGenerator");
            option("-n", "--number PARAGRAPH", "The number of paragraph (default: 2").ofType(int.class).defaultingTo(2);
            operand("out", "OUTPUT", "The destination file").ofType(File.class);
        }};

        try {
            return cli.parse(args);
        } catch (Exception ex) {
            cli.printHelp(System.out);
            System.exit(1);
            return new Args();
        }
    }

    DocumentGenerator(File outPath, int numberOfParagraph) {
        this.outputPath = outPath;
        this.numberOfParagraph = numberOfParagraph;
    }

    public void generate() throws IOException {
        StringJoiner text = new StringJoiner("\n\n");
        for (int i = 0; i < numberOfParagraph; i++) {
            text.add(wordGenerator.nextParagraph());
        }
        documentWriter.write(outputPath.toPath(), text.toString());
    }
}
