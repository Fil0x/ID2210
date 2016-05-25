/*
 * 2016 Royal Institute of Technology (KTH)
 *
 * LSelector is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.kth.news.core.news;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.news.core.SubComponent;
import se.kth.news.core.Utils;
import se.kth.news.core.epfd.MonitorPort;
import se.kth.news.core.epfd.Restore;
import se.kth.news.core.epfd.Suspect;
import se.kth.news.core.leader.LeaderPull;
import se.kth.news.core.leader.LeaderSelectPort;
import se.kth.news.core.leader.LeaderUpdate;
import se.kth.news.core.news.util.NewsView;
import se.kth.news.newsitem.Ping;
import se.kth.news.newsitem.Pong;
import se.kth.news.sim.ScenarioSetup;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.croupier.event.CroupierSample;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.gradient.event.TGradientSample;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.other.Container;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdate;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;

public class NewsComp extends SubComponent {

    private static final Logger LOG = LoggerFactory.getLogger(NewsComp.class);
    private String logPrefix = " ";

    //*******************************CONNECTIONS********************************
    Positive<CroupierPort> croupierPort = requires(CroupierPort.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Positive<LeaderSelectPort> leaderPort = requires(LeaderSelectPort.class);
    Positive<MonitorPort> monitorPort = requires(MonitorPort.class);
    Negative<OverlayViewUpdatePort> viewUpdatePort = provides(OverlayViewUpdatePort.class);
    //*******************************EXTERNAL_STATE*****************************
    private Identifier gradientOId;
    //*******************************INTERNAL_STATE*****************************
    private List<Container<KAddress, NewsView>> acquaintances;
    private List<Container<KAddress, NewsView>> neighbors;
    private int sequenceNumber = -1;
    private KAddress leaderAdr;
    private Map<Integer, Set<String>> newsCoverage = new HashMap<>();  // news item -> {nodes}
    private Map<String, Set<Integer>> nodeKnowledge = new HashMap<>(); // node -> {news items}

    private Set<String> news2 = new HashSet<>();
    private Map<String, KAddress> news2KAddress = new HashMap<>();
    private Map<String, Integer> news2SeqNum = new HashMap<>();

    private Set<KAddress> suspected = new HashSet<>();
    private Map<String, Ping> unconfirmed = new HashMap<>(); // newsId -> news

    public NewsComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.debug("{}initiating...", logPrefix);

        gradientOId = init.gradientOId;

        subscribe(handleStart, control);
        subscribe(handleCroupierSample, croupierPort);
        subscribe(handleGradientSample, gradientPort);
        subscribe(handleLeader, leaderPort);
        subscribe(handleSuspect, monitorPort);
        subscribe(handleRestore, monitorPort);
        subscribe(handlePing, networkPort);
        subscribe(handlePong, networkPort);
        subscribe(handleNewsPull, networkPort);
        subscribe(handleNewsPush, networkPort);
    }

    //*******************************HANDLERS***********************************
    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.debug("{}starting...", logPrefix);
            updateLocalNewsView();
        }
    };

    Handler handleCroupierSample = new Handler<CroupierSample<NewsView>>() {
        @Override
        public void handle(CroupierSample<NewsView> castSample) {
            // not used
        }
    };

    Handler handleGradientSample = new Handler<TGradientSample>() {
        @Override
        public void handle(TGradientSample sample) {
            sequenceNumber += 1;

            acquaintances = Utils.merge(sample.getGradientFingers(), sample.getGradientNeighbours());

            if (leaderAdr != null) {
                resend();
                newsPull();

                if (selfAdr.getId().toString().equals("1")) {
                    // Print results
                    int numberOfNews = newsCoverage.keySet().size();
                    if (numberOfNews > 0) {

                        double coverageSum = 0;
                        for (int newsItem : newsCoverage.keySet()) {
                            int nodes = newsCoverage.get(newsItem).size();
                            double nodesPercent = 100 * nodes / ScenarioSetup.NUMBER_OF_NODES;
                            coverageSum += nodesPercent;
                        }

                        double knowledgeSum = 0;
                        List<Integer> knowledgeList = new LinkedList<>();
                        for (String node : nodeKnowledge.keySet()) {
                            int news = nodeKnowledge.get(node).size();
                            double newsPercent = 100 * news / numberOfNews;
                            knowledgeSum += newsPercent;
                            knowledgeList.add((int) newsPercent);
                        }

                        /*System.out.println("\nnumber of news\t" + numberOfNews);
                        System.out.println("news coverage\t" + coverageSum / numberOfNews);
                        System.out.println("node knowledge\t" + knowledgeSum / NUMBER_OF_NODES);
                        System.out.println("for each node\t" + knowledgeList);*/

                        System.out.println("unconfirmed: " + unconfirmed.size());
                    }

                    if (sequenceNumber < 303) {
                        newsCoverage.put(sequenceNumber, new HashSet<String>());
                        Ping ping = new Ping(selfAdr, sequenceNumber, null, ScenarioSetup.TTL);
                        unconfirmed.put(ping.getIdentifier(), ping);
                        triggerSend(leaderAdr, ping);
                    }
                }
            }
        }
    };

    Handler handleLeader = new Handler<LeaderUpdate>() {
        @Override
        public void handle(LeaderUpdate event) {
            leaderAdr = event.leaderAdr;
            //LOG.info("{} new leader: {}", logPrefix, leaderAdr.getId());
        }
    };

    Handler handleSuspect = new Handler<Suspect>() {
        @Override
        public void handle(Suspect event) {
            if (suspected.add(event.node)) {
                LOG.info("{} suspect: {}", logPrefix, event.node.getId());
            }
        }
    };

    Handler handleRestore = new Handler<Restore>() {
        @Override
        public void handle(Restore event) {
            if (suspected.remove(event.node)) {
                LOG.info("{} restore: {}", logPrefix, event.node.getId());
            }
        }
    };

    ClassMatchedHandler handlePing
            = new ClassMatchedHandler<Ping, KContentMsg<?, ?, Ping>>() {

        @Override
        public void handle(Ping content, KContentMsg<?, ?, Ping> container) {
            KAddress source = container.getHeader().getSource();
            LOG.debug("{} received ping from: {}", logPrefix, source.getId());
            String newsItem = content.getIdentifier();
            news2KAddress.put(newsItem, content.origin);
            news2SeqNum.put(newsItem, content.seqNum);
            news2.add(newsItem);
            updateLocalNewsView();

            // Send Pong
            triggerSend(news2KAddress.get(newsItem), new Pong(news2SeqNum.get(newsItem)));
        }
    };

    ClassMatchedHandler handlePong
            = new ClassMatchedHandler<Pong, KContentMsg<?, KHeader<?>, Pong>>() {

        @Override
        public void handle(Pong content, KContentMsg<?, KHeader<?>, Pong> container) {
            KAddress source = container.getHeader().getSource();
            LOG.debug("{}received pong from:{}", logPrefix, source.getId());
            String sourceId = source.getId().toString();
            newsCoverage.get(content.seqNum).add(sourceId);
            if (nodeKnowledge.get(sourceId) == null) {
                nodeKnowledge.put(sourceId, new HashSet<Integer>());
            }
            nodeKnowledge.get(sourceId).add(content.seqNum);
        }
    };

    ClassMatchedHandler handleNewsPull
            = new ClassMatchedHandler<NewsPull, KContentMsg<?, ?, NewsPull>>() {
        @Override
        public void handle(NewsPull content, KContentMsg<?, ?, NewsPull> container) {
            KAddress source = container.getHeader().getSource();
            triggerSend(source, new NewsPush(news2, news2KAddress, news2SeqNum));
        }
    };

    ClassMatchedHandler handleNewsPush
            = new ClassMatchedHandler<NewsPush, KContentMsg<?, ?, NewsPush>>() {
        @Override
        public void handle(NewsPush content, KContentMsg<?, ?, NewsPush> container) {
            for (String newsItem : container.getContent().news2) {
                news2KAddress.put(newsItem, container.getContent().news2KAddress.get(newsItem));
                news2SeqNum.put(newsItem, container.getContent().news2SeqNum.get(newsItem));
                news2.add(newsItem);
                updateLocalNewsView();

                // Send Pong
                triggerSend(news2KAddress.get(newsItem), new Pong(news2SeqNum.get(newsItem)));

                unconfirmed.remove(newsItem);
            }
        }
    };

    //*******************************HELP_FUNCTIONS*****************************
    private void updateLocalNewsView() {
        int utility = news2.size();
        if (selfAdr.getId().toString().equals("3")) {
            utility += 450;
        }
        if (selfAdr.getId().toString().equals("5")) {
            utility += 500;
        }
        NewsView localNewsView = new NewsView(selfAdr.getId(), utility);
        LOG.debug("{}informing overlays of new view", logPrefix);
        trigger(new OverlayViewUpdate.Indication<>(gradientOId, false, localNewsView.copy()), viewUpdatePort);
    }

    private void newsPull() {
        Container<KAddress, NewsView> maxRank = Utils.maxRank(acquaintances, suspected);
        if (maxRank != null) {
            triggerSend(maxRank.getSource(), new NewsPull());
        }
    }

    private void resend() {
        for (String newsId : unconfirmed.keySet()) {
            triggerSend(leaderAdr, unconfirmed.get(newsId));
        }
    }

    public static class Init extends se.sics.kompics.Init<NewsComp> {

        public final KAddress selfAdr;
        public final Identifier gradientOId;

        public Init(KAddress selfAdr, Identifier gradientOId) {
            this.selfAdr = selfAdr;
            this.gradientOId = gradientOId;
        }
    }
}
