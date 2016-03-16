package org.bjean.sample.wordcount.input;

import java.util.Random;
import java.util.StringJoiner;

public class RandomWordGenerator {

    private final Random random;

    public RandomWordGenerator(Random random) {
        this.random = random;
    }

    public String nextWord() {
        char[] word = new char[random.nextInt(8) + 5]; // words of length 5 through 13. (1 and 2 letter words are boring.)
        for (int j = 0; j < word.length; j++) {
            word[j] = (char) ('a' + random.nextInt(26));
        }
        return new String(word);
    }
    
    public String nextParagraph() {
        StringJoiner joiner = new StringJoiner(" ");
        for (int i = 0; i < random.nextInt(20) + 2; i++) {
            joiner.add(nextSentence());
        }
        return "\t"+joiner.toString();
    }

    public String nextSentence() {
        StringJoiner joiner = new StringJoiner(" ");
        for (int i = 0; i < random.nextInt(8) + 2; i++) {
            joiner.add(nextWord());
        }
        return joiner.toString() + ".";
    }
}
