package nyse.parsers;

public class CompanyParser {
	String stockTicker;
	String name;
	String lastSale;
	String marketCap;
	String adrsto;
	String ipoYear;
	String sector;
	String industry;
	String summaryQuote;
	
	public void parse(String record) {
		String[] vals = record.split("\\|");
		
		this.stockTicker = vals[0];
		this.name = vals[1];
		this.lastSale = new String(vals[2]);
		this.marketCap = new String(vals[3]);
		this.adrsto = new String(vals[4]);
		this.ipoYear = new String(vals[5]);
		this.sector = new String(vals[6]);
		this.industry = new String(vals[7]);
		this.summaryQuote = new String(vals[8]);				
	}
	
	public CompanyParser(String record) {
		parse(record);
	}
	
	public String getStockTicker() {
		return stockTicker;
	}
	public void setStockTicker(String stockTicker) {
		this.stockTicker = stockTicker;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getLastSale() {
		return lastSale;
	}
	public void setLastSale(String lastSale) {
		this.lastSale = lastSale;
	}
	public String getMarketCap() {
		return marketCap;
	}
	public void setMarketCap(String marketCap) {
		this.marketCap = marketCap;
	}
	public String getAdrsto() {
		return adrsto;
	}
	public void setAdrsto(String adrsto) {
		this.adrsto = adrsto;
	}
	public String getIpoYear() {
		return ipoYear;
	}
	public void setIpoYear(String ipoYear) {
		this.ipoYear = ipoYear;
	}
	public String getSector() {
		return sector;
	}
	public void setSector(String sector) {
		this.sector = sector;
	}
	public String getIndustry() {
		return industry;
	}
	public void setIndustry(String industry) {
		this.industry = industry;
	}
	public String getSummaryQuote() {
		return summaryQuote;
	}
	public void setSummaryQuote(String summaryQuote) {
		this.summaryQuote = summaryQuote;
	}
}
