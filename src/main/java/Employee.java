import java.io.Serializable;

public class Employee implements Serializable {

    private int year;

    private String name;

    @Override
    public String toString() {
        return "Employee{" +
                "year=" + year +
                ", name='" + name + '\'' +
                ", country='" + country + '\'' +
                '}';
    }

    private String country;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }
}
