package se.kth.news.core.leader;

import se.sics.ktoolbox.util.network.KAddress;

public class LeaderPush {

    public final KAddress leaderAdr;

    public LeaderPush(KAddress leaderAdr) {
        this.leaderAdr = leaderAdr;
    }
}
