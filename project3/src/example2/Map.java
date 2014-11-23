package example2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import mapredCommon.Mapper;
import mapredCommon.RecordWriter;

public class Map extends Mapper {
	HashSet<String> stopWords = new HashSet<String>();
	
	private final String STOP_WORDS_PATH = "../testdata/english.stop.txt";
	
	@Override
	public void setup() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(STOP_WORDS_PATH));
			String line;
			while ((line = reader.readLine()) != null) {
				stopWords.add(line);
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public void map(int key, String value, RecordWriter writer) {
		String[] words = value.split("\\W+");
		for (String word : words) {
			word = word.toLowerCase();
			if (stopWords.contains(word)) {
				continue;
			}
			try {
				writer.write(word, "1");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
