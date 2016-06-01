package se.kth.news.core.leader;

public class Leader2PC {

    public final int sid; // session id
    public final String header;
    public final Object body;

    public Leader2PC(int sid, String header, Object body) {
        this.sid = sid;
        this.header = header;
        this.body = body;
    }
}
