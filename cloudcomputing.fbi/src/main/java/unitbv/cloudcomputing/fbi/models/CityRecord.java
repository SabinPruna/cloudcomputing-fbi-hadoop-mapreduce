package unitbv.cloudcomputing.fbi.models;

public class CityRecord {

	public String name;

	public int population;

	public int murders;

	public int rapes;

	public int robberies;

	public int aggravatedAssaults;

	public int propertyCrimes;

	public int burglaries;

	public int larcenyThefts;			

	public int motorVehicleThefts;
	
	/**
	 * Constructor
	 * @param name
	 * @param population
	 * @param murders
	 * @param rapes
	 * @param robberies
	 * @param aggravatedAssaults
	 * @param propertyCrimes
	 * @param burglaries
	 * @param larcenyThefts
	 * @param motorVehicleThefts
	 */
	public CityRecord(String name, int population, int murders, int rapes, int robberies, int aggravatedAssaults,
			int propertyCrimes, int burglaries, int larcenyThefts, int motorVehicleThefts) {
		super();
		this.name = name;
		this.population = population;
		this.murders = murders;
		this.rapes = rapes;
		this.robberies = robberies;
		this.aggravatedAssaults = aggravatedAssaults;
		this.propertyCrimes = propertyCrimes;
		this.burglaries = burglaries;
		this.larcenyThefts = larcenyThefts;
		this.motorVehicleThefts = motorVehicleThefts;
	}

	//Methods, Getters, Setters
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CityRecord other = (CityRecord) obj;
		if (aggravatedAssaults != other.aggravatedAssaults)
			return false;
		if (burglaries != other.burglaries)
			return false;
		if (larcenyThefts != other.larcenyThefts)
			return false;
		if (motorVehicleThefts != other.motorVehicleThefts)
			return false;
		if (murders != other.murders)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (population != other.population)
			return false;
		if (propertyCrimes != other.propertyCrimes)
			return false;
		if (rapes != other.rapes)
			return false;
		if (robberies != other.robberies)
			return false;
		return true;
	}

	public int getAggravatedAssaults() {
		return aggravatedAssaults;
	}

	public int getBurglaries() {
		return burglaries;
	}

	public int getLarcenyThefts() {
		return larcenyThefts;
	}

	public int getMotorVehicleThefts() {
		return motorVehicleThefts;
	}

	public int getMurders() {
		return murders;
	}

	public String getName() {
		return name;
	}

	public int getPopulation() {
		return population;
	}

	public int getPropertyCrimes() {
		return propertyCrimes;
	}

	public int getRapes() {
		return rapes;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + aggravatedAssaults;
		result = prime * result + burglaries;
		result = prime * result + larcenyThefts;
		result = prime * result + motorVehicleThefts;
		result = prime * result + murders;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + population;
		result = prime * result + propertyCrimes;
		result = prime * result + rapes;
		result = prime * result + robberies;
		return result;
	}

	public void setAggravatedAssaults(int aggravatedAssaults) {
		this.aggravatedAssaults = aggravatedAssaults;
	}

	public void setBurglaries(int burglaries) {
		this.burglaries = burglaries;
	}

	public void setLarcenyThefts(int larcenyThefts) {
		this.larcenyThefts = larcenyThefts;
	}

	public void setMotorVehicleThefts(int motorVehicleThefts) {
		this.motorVehicleThefts = motorVehicleThefts;
	}

	public void setMurders(int murders) {
		this.murders = murders;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setPopulation(int population) {
		this.population = population;
	}

	public void setPropertyCrimes(int propertyCrimes) {
		this.propertyCrimes = propertyCrimes;
	}

	public void setRapes(int rapes) {
		this.rapes = rapes;
	}

	@Override
	public String toString() {
		return "CityRecord [name=" + name + ", population=" + population + ", murders=" + murders + ", rapes=" + rapes
				+ ", robberies=" + robberies + ", aggravatedAssaults=" + aggravatedAssaults + ", propertyCrimes="
				+ propertyCrimes + ", burglaries=" + burglaries + ", larcenyThefts=" + larcenyThefts
				+ ", motorVehicleThefts=" + motorVehicleThefts + "]";
	}

}
