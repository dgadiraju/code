package nyse.keyvalues;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class NYSEWritable implements Writable {
	private Text stockTicker;
	private Text tradeDate;
	private FloatWritable openPrice;
	private FloatWritable highPrice;
	private FloatWritable lowPrice;
	private FloatWritable closePrice;
	private LongWritable volume;

	public NYSEWritable() {
		super();
		stockTicker = new Text();
		tradeDate = new Text();
		openPrice = new FloatWritable();
		highPrice = new FloatWritable();
		lowPrice = new FloatWritable();
		closePrice = new FloatWritable();
		volume = new LongWritable();
	}

	public Text getStockTicker() {
		return stockTicker;
	}

	public void setStockTicker(Text stockTicker) {
		this.stockTicker = stockTicker;
	}

	public Text getTradeDate() {
		return tradeDate;
	}

	public void setTradeDate(Text tradeDate) {
		this.tradeDate = tradeDate;
	}

	public FloatWritable getOpenPrice() {
		return openPrice;
	}

	public void setOpenPrice(FloatWritable openPrice) {
		this.openPrice = openPrice;
	}

	public FloatWritable getHighPrice() {
		return highPrice;
	}

	public void setHighPrice(FloatWritable highPrice) {
		this.highPrice = highPrice;
	}

	public FloatWritable getLowPrice() {
		return lowPrice;
	}

	public void setLowPrice(FloatWritable lowPrice) {
		this.lowPrice = lowPrice;
	}

	public FloatWritable getClosePrice() {
		return closePrice;
	}

	public void setClosePrice(FloatWritable closePrice) {
		this.closePrice = closePrice;
	}

	public LongWritable getVolume() {
		return volume;
	}

	public void setVolume(LongWritable volume) {
		this.volume = volume;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		stockTicker.readFields(arg0);
		tradeDate.readFields(arg0);
		openPrice.readFields(arg0);
		highPrice.readFields(arg0);
		lowPrice.readFields(arg0);
		closePrice.readFields(arg0);
		volume.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		stockTicker.write(arg0);
		tradeDate.write(arg0);
		openPrice.write(arg0);
		highPrice.write(arg0);
		lowPrice.write(arg0);
		closePrice.write(arg0);
		volume.write(arg0);
	}

	@Override
	public String toString() {
		return stockTicker 
				+ "\t" + tradeDate 
				+ "\t" + openPrice
				+ "\t" + highPrice
				+ "\t" + lowPrice
				+ "\t" + closePrice
				+ "\t" + volume;
	}

}
