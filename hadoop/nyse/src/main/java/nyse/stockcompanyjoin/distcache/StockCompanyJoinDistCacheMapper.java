package nyse.stockcompanyjoin.distcache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import nyse.keyvalues.TextPair;
import nyse.parsers.CompanyParser;
import nyse.parsers.NYSEParser;

public class StockCompanyJoinDistCacheMapper extends Mapper<LongWritable, Text, TextPair, Text> {

	private static NYSEParser parser = new NYSEParser();
	private static TextPair mapOutputKey = new TextPair();
	private static Text mapOutputValue = new Text();

	private static CompanyParser companyParser = null;
	private Map<String, CompanyParser> stockAndCompanyDetails = new HashMap<String, CompanyParser>();

	void initialize() throws IOException {
		File companyDataFile = new File("companylist_noheader.csv");
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(companyDataFile)));
			String line;
			while ((line = in.readLine()) != null) {
				companyParser = new CompanyParser(line);
				stockAndCompanyDetails.put(companyParser.getStockTicker(), companyParser);
			}

		} finally {
			IOUtils.closeStream(in);
		}

	}

	protected void setup(Context context) throws IOException, InterruptedException {
		initialize();
	}

	public void map(LongWritable lineOffset, Text record, Context context) throws IOException, InterruptedException {
		parser.parse(record.toString());

		mapOutputKey = new TextPair(new Text(parser.getStockTicker()), new Text(parser.getTradeDate()));

		CompanyParser company = null;

		company = stockAndCompanyDetails.get(parser.getStockTicker());

		if (company != null) {
			String companyName = company.getName().replace(",", "");
			String industry = company.getIndustry();
			String sector = company.getSector();

			mapOutputValue = new Text(record.toString() + "," + companyName + "," + industry + "," + sector);

			context.write(mapOutputKey, mapOutputValue);
		}

	}

}
