package Spark_Scala_tasks.exercise01;

import java.io.*;
import java.util.HashMap;

public class JavaWordCount {
    public static void main(String[] args) throws IOException {
        // create a new file object with importing the text file
        File file = new File("in/word_count.text");
        // create a New InputStream, InputStreamReader and a BufferReader for File Read, I/O Operations
        FileInputStream fileStream = new FileInputStream(file);
        InputStreamReader input = new InputStreamReader(fileStream);
        BufferedReader reader = new BufferedReader(input);

        // initializing line
        String line;

        // initializing a hashMap to store word counts
        HashMap<String, Integer> wordCounts = new HashMap<String, Integer>();

        // Initializing counters
        int wordCount = 0;
        int characterCount = 0;
        int sentenceCount = 1;

        // Reading line by line from the
        // file until a null is returned
        while ((line = reader.readLine()) != null) {

            characterCount += line.length();

            // \\s+ is the space delimiter in java
            String[] wordList = line.split("\\s+");

            // Iterate through wordList to count unique ords
            for (String item : wordList) {
                if(!wordCounts.containsKey(item)){ //add the word if it isn't added already
                    wordCounts.put(item, 1);
                } else {
                    wordCounts.put(item, wordCounts.get(item) + 1);
                }
            }

            // total count of words
            wordCount += wordList.length;

            // [!?.:]+ is the sentence delimiter in java
            String[] sentenceList = line.split("[!?.:]+");
            sentenceCount += sentenceList.length;

        }

        // print word counts
        System.out.println("Total no of Words = " + wordCount);
        System.out.println("Total no of sentences = " + sentenceCount);
        System.out.println("Total no of characters = " + characterCount +'\n');
        System.out.println("Word Counts : \n" + wordCounts);
    }
}

