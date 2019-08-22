package in.injulkar.nilesh.boot.awsstory.controllers.models;

public class Student {
    private Long id;
    private String firstName;
    private String lastName;

    public Long getId() {
        return id;
    }

    public void setId(final Long id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(final String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(final String lastName) {
        this.lastName = lastName;
    }

    @Override
    public String toString() {
        return new StringBuilder(128)
                .append(" ID: ").append(this.id)
                .append(" FirstName: ").append(this.firstName)
                .append(" LastName: ").append(this.lastName)
                .toString();
    }
}
