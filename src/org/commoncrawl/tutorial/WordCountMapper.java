package org.commoncrawl.tutorial;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.jsoup.Jsoup;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.apache.commons.lang.StringUtils;

/**
 * Outputs all words contained within the displayed text of pages contained
 * within {@code ArcFileItem} objects.
 * 
 * @author Steve Salevan <steve.salevan@gmail.com>
 */
public class WordCountMapper extends MapReduceBase 
  implements Mapper<Text, ArcFileItem, Text, LongWritable> {
  private final static Pattern REGEX_NON_ALPHANUMERIC = Pattern.compile("[^a-zA-Z0-9 ]");
  private final static Pattern REGEX_SPACE = Pattern.compile("\\s+");

  public void map(Text key, ArcFileItem value,
      OutputCollector<Text, LongWritable> output, Reporter reporter)
      throws IOException {
    try {
      if (!value.getMimeType().contains("pdf")) {
        return;  // Only parse pdfs.
      }
      // Retrieves page content from the passed-in ArcFileItem.
      ByteArrayInputStream inputStream = new ByteArrayInputStream(
          value.getContent().getReadOnlyBytes(), 0,
          value.getContent().getCount());
		  
      String pageText = new String();
			try {
				pageText = parse(inputStream);
			} catch (Throwable t) {
				err.println("Could not parse document:" + t.getClass() + ":"
						+ t.getMessage());
				t.printStackTrace(err);
			}
      // Removes all punctuation.
      pageText = REGEX_NON_ALPHANUMERIC.matcher(pageText).replaceAll("");
      // Normalizes whitespace to single spaces.
      pageText = pageText.replaceAll("\\s+", " ");
      String[] words=new String[500];
	  int x = 0;
	  for (String word : pageText.split(" ")) {
		if (x<500){
		  words[x]=word;
		  x++;
		}
		else{
		  break;
		}
	  }
	  pageText = StringUtils.join(words," ");
	  output.collect(new Text(pageText), new LongWritable(1));
	  String[] arguments = new String[8];
	  arguments[0]="--path https://s3.amazonaws.com/";
	  arguments[1]=pageText;
	  arguments[2]="--encoding UTF-8";
	  //arguments[3]="analyzer";
	  arguments[4]="--defaultCat unknown";
	  arguments[5]="--gramSize 1";
	  arguments[6]="--classifierType bayes";
	  arguments[7]="--dataSource hdfs";
    }
    catch (Exception e) {
      reporter.getCounter("WordCountMapper.exception",
          e.getClass().getSimpleName()).increment(1);
    }
  }
  private static String parse(ByteArrayInputStream input1)
	  throws IOException, SAXException, TikaException {

	InputStream input = input1;
	ContentHandler textHandler = new BodyContentHandler();
	Metadata metadata = new Metadata();
	PDFParser parser = new PDFParser();
	parser.parse(input, textHandler, metadata);
	input.close();
	return textHandler.toString();
  }
}
