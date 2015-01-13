package demo.cards.parsers;

/**
 * Created with IntelliJ IDEA. User: kavithapenmetsa Date: 10/20/13 Time: 11:06
 * AM To change this template use File | Settings | File Templates.
 */
public class CardParser {
	private String color;
	private String suit;
	private String pip;

	public void parse(String record) {
		String[] vals = record.split("\\|");
		color = vals[0];
		suit = vals[1];
		pip = vals[2];
	}

	public String getColor() {
		return color;
	}

	public void setColor(String color) {
		this.color = color;
	}

	public String getSuit() {
		return suit;
	}

	public void setSuit(String suit) {
		this.suit = suit;
	}

	public String getPip() {
		return pip;
	}

	public void setPip(String pip) {
		this.pip = pip;
	}

	public String getColorAndSuit() {
		return color + "|" + suit;
	}

}
