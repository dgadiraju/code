package analytics.nyse.hive.udfs;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class DateTranslate extends UDF {
    public String evaluate(Text str) {
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
		//03-Jan-2013
		String transactionDate = null;
		try {
			transactionDate = (new SimpleDateFormat("yyyy-MM-dd") //2013-01-03
					.format(formatter.parse(str.toString())))
					.toString();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return transactionDate;
		
    }

}
