package se.kth.news.core.epfd;

import se.sics.kompics.KompicsEvent;
import se.sics.ktoolbox.util.network.KAddress;

public class Suspect implements KompicsEvent {

    public final KAddress node;

    public Suspect(KAddress node) {
        this.node = node;
    }
}
