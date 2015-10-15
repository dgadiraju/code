
public class NyseParser {
	String stockTicker;
	String transactionDate;
	Float openPrice;
	Float highPrice;
	Float lowPrice;
	Float closePrice;
	Integer volume;
	
	public void parse(String record) {
		String[] vals = record.split(",");
        stockTicker = vals[0];
        transactionDate = vals[1];
        openPrice = new Float(vals[2]); 
        highPrice = new Float(vals[3]);
        lowPrice = new Float(vals[4]);
        closePrice = new Float(vals[5]);
        volume = new Integer(vals[6]);
	}
	
	public String getStockTicker() {
		return stockTicker;
	}
	public void setStockTicker(String stockTicker) {
		this.stockTicker = stockTicker;
	}
	public String getTransactionDate() {
		return transactionDate;
	}
	public void setTransactionDate(String transactionDate) {
		this.transactionDate = transactionDate;
	}
	public String getYear() {
		String year = "";
		year = transactionDate.substring(7);
		return year;
	}
	public Float getOpenPrice() {
		return openPrice;
	}
	public void setOpenPrice(Float openPrice) {
		this.openPrice = openPrice;
	}
	public Float getHighPrice() {
		return highPrice;
	}
	public void setHighPrice(Float highPrice) {
		this.highPrice = highPrice;
	}
	public Float getLowPrice() {
		return lowPrice;
	}
	public void setLowPrice(Float lowPrice) {
		this.lowPrice = lowPrice;
	}
	public Float getClosePrice() {
		return closePrice;
	}
	public void setClosePrice(Float closePrice) {
		this.closePrice = closePrice;
	}
	public Integer getVolume() {
		return volume;
	}
	public void setVolume(Integer volume) {
		this.volume = volume;
	}

}
