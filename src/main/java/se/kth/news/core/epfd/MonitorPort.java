package se.kth.news.core.epfd;

import se.sics.kompics.PortType;

public class MonitorPort extends PortType {
    {
        request(MonitorRequest.class);
        indication(Suspect.class);
        indication(Restore.class);
    }
}
