package analytics.nyse.hive.udfs;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class DateTranslate extends UDF {
    public String evaluate(Text str) {
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
		String transactionDate = null;
		try {
			transactionDate = (new SimpleDateFormat("yyyy-MM-dd")
					.format(formatter.parse(str.toString())))
					.toString();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return transactionDate;
		
    }

}
