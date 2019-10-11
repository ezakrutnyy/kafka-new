package kafka.value;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.sun.javafx.UnmodifiableArrayList;
import org.apache.commons.collections.list.UnmodifiableList;

import java.util.LinkedHashMap;
import java.util.List;

public class Employee {

    String login;

    String country;

    String city;

    String email;

    Integer age;

    public Employee() {
    }

    public Employee(String login, String country, Integer age) {
        this.login = login;
        this.country = country;
        this.age = age;
    }

    public Employee(String login, String country, String city, String email, Integer age) {
        this.login = login;
        this.country = country;
        this.city = city;
        this.email = email;
        this.age = age;
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }


    public static List<String> titles() {
        return ImmutableList.of(
                "Логин",
                "Страна",
                "Город",
                "Email",
                "Возраст"
        );
    }

    public static List<Employee> fillTest() {

        List<Employee> employees = Lists.newArrayList();

        Employee employee = new Employee();
        employee.setLogin("user-1");
        employee.setCountry("Russia");
        employee.setAge(31);
        employee.setCity("Moscow");
        employee.setEmail("user-1@mail.ru");
        employees.add(employee);

        employee = new Employee();
        employee.setLogin("user-2");
        employee.setCountry("Russia");
        employee.setAge(25);
        employee.setCity("Ekaterinburg");
        employee.setEmail("user-2@mail.ru");
        employees.add(employee);

        employee = new Employee();
        employee.setLogin("user-3");
        employee.setCountry("Usa");
        employee.setAge(31);
        employee.setCity("NYC");
        employee.setEmail("user-3@gmail.com");
        employees.add(employee);

        employee = new Employee();
        employee.setLogin("user-4");
        employee.setCountry("Germany");
        employee.setAge(27);
        employee.setCity("Keln");
        employee.setEmail("user-4@mail.ru");
        employees.add(employee);

        employee = new Employee();
        employee.setLogin("user-5");
        employee.setCountry("Italy");
        employee.setAge(42);
        employee.setCity("Palermo");
        employee.setEmail("user-5@mail.ru");
        employees.add(employee);

        return employees;
    }
}