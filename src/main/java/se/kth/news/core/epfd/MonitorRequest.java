package se.kth.news.core.epfd;

import se.sics.kompics.KompicsEvent;
import se.sics.ktoolbox.util.network.KAddress;

import java.util.Set;

public class MonitorRequest implements KompicsEvent {

    public final Set<KAddress> nodes;

    public MonitorRequest(Set<KAddress> nodes) {
        this.nodes = nodes;
    }
}
