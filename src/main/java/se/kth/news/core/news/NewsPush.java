package se.kth.news.core.news;

import se.sics.ktoolbox.util.network.KAddress;

import java.util.Map;
import java.util.Set;

public class NewsPush {
    public final Set<String> news2;
    public final Map<String, KAddress> news2KAddress;
    public final Map<String, Integer> news2SeqNum;

    public NewsPush(Set<String> news2, Map<String, KAddress> news2KAddress, Map<String, Integer> news2SeqNum) {
        this.news2 = news2;
        this.news2KAddress = news2KAddress;
        this.news2SeqNum = news2SeqNum;
    }
}
