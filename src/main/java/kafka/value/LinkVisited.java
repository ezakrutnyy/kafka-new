package kafka.value;

import java.io.Serializable;

public class LinkVisited implements Serializable {

    private final Long range;

    private final String url;

    private final  String login;

    public LinkVisited(Long range, String url, String login) {
        this.range = range;
        this.url = url;
        this.login = login;
    }

    public Long getRange() {
        return range;
    }

    public String getUrl() {
        return url;
    }

    @Override
    public String toString() {
        return "LinkVisited{" +
                "range=" + range +
                ", url='" + url + '\'' +
                ", login='" + login + '\'' +
                '}';
    }

    public String getLogin() {
        return login;
    }
}