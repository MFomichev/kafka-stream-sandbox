package xyz.fomichev;

public class Application {

    private String name;
    private String lastName;
    private String middleName;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getMiddleName() {
        return middleName;
    }

    public void setMiddleName(String middleName) {
        this.middleName = middleName;
    }

    public void setDTO(ApplicationDTO dto) {
        switch (dto.getKey()) {
            case "name":       name        = dto.getValue(); break;
            case "lastName":   lastName    = dto.getValue(); break;
            case "middleName": middleName  = dto.getValue(); break;
        }
    }
}
