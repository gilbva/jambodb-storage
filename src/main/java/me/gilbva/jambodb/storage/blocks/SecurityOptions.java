package me.gilbva.jambodb.storage.blocks;

public class SecurityOptions {
    private String password;

    private String salt;

    public SecurityOptions(String password, String salt) {
        this.password = password;
        this.salt = salt;
    }

    public String password() {
        return password;
    }

    public String salt() {
        return salt;
    }
}
