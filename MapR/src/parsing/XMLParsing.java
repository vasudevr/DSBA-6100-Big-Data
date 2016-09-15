package parsing;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;
import java.util.regex.Pattern;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class XMLParsing {
	static Properties properties;

	public static class XmlInputFormat1 extends TextInputFormat {
		public static final String START_TAG_KEY = "xmlinput.start";
		public static final String END_TAG_KEY = "xmlinput.end";

		public RecordReader<LongWritable, Text> createRecordReader(
				InputSplit split, TaskAttemptContext context) {
			return new XmlRecordReader();
		}

		public static class XmlRecordReader extends
				RecordReader<LongWritable, Text> {
			private byte[] startTag;
			private byte[] endTag;
			private long start;
			private long end;
			private FSDataInputStream fsin;
			private DataOutputBuffer buffer = new DataOutputBuffer();
			private LongWritable key = new LongWritable();
			private Text value = new Text();

			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				this.startTag = conf.get("xmlinput.start").getBytes("utf-8");
				this.endTag = conf.get("xmlinput.end").getBytes("utf-8");
				FileSplit fileSplit = (FileSplit) split;

				this.start = fileSplit.getStart();
				this.end = (this.start + fileSplit.getLength());
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(conf);
				this.fsin = fs.open(fileSplit.getPath());
				this.fsin.seek(this.start);
			}

			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				if ((this.fsin.getPos() < this.end)
						&& (readUntilMatch(this.startTag, false))) {
					try {
						this.buffer.write(this.startTag);
						if (readUntilMatch(this.endTag, true)) {
							this.key.set(this.fsin.getPos());
							this.value.set(this.buffer.getData(), 0,
									this.buffer.getLength());
							return true;
						}
					} finally {
						this.buffer.reset();
					}
					this.buffer.reset();

					this.buffer.reset();
				}
				return false;
			}

			public LongWritable getCurrentKey() throws IOException,
					InterruptedException {
				return this.key;
			}

			public Text getCurrentValue() throws IOException,
					InterruptedException {
				return this.value;
			}

			public void close() throws IOException {
				this.fsin.close();
			}

			public float getProgress() throws IOException {
				return (float) (this.fsin.getPos() - this.start)
						/ (float) (this.end - this.start);
			}

			private boolean readUntilMatch(byte[] match, boolean withinBlock)
					throws IOException {
				int i = 0;
				do {
					int b = this.fsin.read();
					if (b == -1) {
						return false;
					}
					if (withinBlock) {
						this.buffer.write(b);
					}
					if (b == match[i]) {
						i++;
						if (i >= match.length) {
							return true;
						}
					} else {
						i = 0;
					}
				} while ((withinBlock) || (i != 0)
						|| (this.fsin.getPos() < this.end));
				return false;
			}
		}
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		URI[] path;
		
				@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			URI[] path = context.getCacheFiles();
			properties = new Properties();
			InputStream stream = null;
			super.setup(context);
			File file = new File(path[0]);
			stream = new FileInputStream(file);
			properties.load(stream);
			stream.close();
		}



		@SuppressWarnings({ "unchecked", "rawtypes" })
		protected void map(LongWritable key, Text value, Mapper.Context context)
				throws IOException, InterruptedException {
			String document = value.toString();
			
			try {
				XMLStreamReader reader = XMLInputFactory.newInstance()
						.createXMLStreamReader(
								new ByteArrayInputStream(document.getBytes()));
				String currentElement = "";
				String propertyName = "";
				while (reader.hasNext()) {
					int code = reader.next();
					switch (code) {
					case 1:
						currentElement = reader.getLocalName();
						break;
					case 4:
						if (currentElement.equalsIgnoreCase("orgname")) {
							propertyName = reader.getText();
							propertyName = propertyName.trim();
							propertyName = propertyName.toLowerCase();
							Pattern REPLACE = Pattern
									.compile("[][.,';?!#$%<>/(){}]\\s]");
							String rawtext = REPLACE.matcher(propertyName)
									.replaceAll("");
							if (properties.containsKey(rawtext)) {
								this.word.set(properties.getProperty(rawtext));
								context.write(this.word, one);
							}
						}
						break;
					}
				}
				reader.close();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		conf.set("xmlinput.start", "<assignees>");
		conf.set("xmlinput.end", "</assignees>");
		Job job = Job.getInstance(conf);
		
		File file = new File("fortuneData.properties");
		job.addCacheFile(file.toURI());
		job.setJarByClass(XMLParsing.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(XmlInputFormat1.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}