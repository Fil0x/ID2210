package se.kth.news.core.leader;

import se.sics.ktoolbox.util.network.KAddress;

import java.io.Serializable;

public class LeaderPush implements Serializable {
    public final KAddress leaderAdr;

    public LeaderPush(KAddress leaderAdr) {
        this.leaderAdr = leaderAdr;
    }
}
