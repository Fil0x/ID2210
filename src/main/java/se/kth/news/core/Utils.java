package se.kth.news.core;

import se.kth.news.core.news.util.NewsView;
import se.kth.news.core.news.util.NewsViewComparator;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.other.Container;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Utils {

    private static Comparator viewComparator = new NewsViewComparator();

    public static Set<KAddress> addressSet(List<Container<KAddress, NewsView>> containerList) {
        Set<KAddress> addressSet = new HashSet<>();
        for (Container<KAddress, NewsView> c : containerList) {
            addressSet.add(c.getSource());
        }
        return addressSet;
    }

    public static Container<KAddress, NewsView> maxRank(List<Container<KAddress, NewsView>> nodes) {
        return maxRank(nodes, new HashSet<KAddress>());
    }

    public static Container<KAddress, NewsView> maxRank(List<Container<KAddress, NewsView>> nodes, Set<KAddress> ignore) {
        Container<KAddress, NewsView> maxRank = null;
        for (Container<KAddress, NewsView> c : nodes) {
            if (!ignore.contains(c.getSource())) {
                if (maxRank == null || viewComparator.compare(maxRank.getContent(), c.getContent()) < 0) {
                    maxRank = c;
                }
            }
        }
        return maxRank;
    }

    public static Set intersection(Set s1, Set s2) {
        Set intersection = new HashSet(s1);
        intersection.retainAll(s2);
        return intersection;
    }

    public static Set union(Set s1, Set s2) {
        Set union = new HashSet(s1);
        union.addAll(s2);
        return union;
    }
}
