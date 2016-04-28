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
import se.kth.news.core.leader.LeaderSelectPort;
import se.kth.news.core.leader.LeaderUpdate;
import se.kth.news.core.news.util.NewsView;
import se.kth.news.play.Ping;
import se.kth.news.play.Pong;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.croupier.event.CroupierSample;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.gradient.event.TGradientSample;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdate;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;

public class NewsComp extends ComponentDefinition {

    //*******************************NODE_SETUP*********************************
    public static final int NUMBER_OF_NODES = 100;
    public static final int TTL = 5;
    //*******************************LOGGING************************************
    private static final Logger LOG = LoggerFactory.getLogger(NewsComp.class);
    private String logPrefix = " ";
    //*******************************CONNECTIONS********************************
    Positive<Timer> timerPort = requires(Timer.class);
    Positive<Network> networkPort = requires(Network.class);
    Positive<CroupierPort> croupierPort = requires(CroupierPort.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Positive<LeaderSelectPort> leaderPort = requires(LeaderSelectPort.class);
    Negative<OverlayViewUpdatePort> viewUpdatePort = provides(OverlayViewUpdatePort.class);
    //*******************************EXTERNAL_STATE*****************************
    private KAddress selfAdr;
    private Identifier gradientOId;
    //*******************************INTERNAL_STATE*****************************
    private NewsView localNewsView;
    private CroupierSample<NewsView> croupierSample;
    private int sequenceNumber = 0;
    private Map<Integer, Set<String>> newsCoverage = new HashMap<>();  // news item -> {nodes}
    private Map<String, Set<Integer>> nodeKnowledge = new HashMap<>(); // node -> {news items}

    public NewsComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}initiating...", logPrefix);

        gradientOId = init.gradientOId;

        subscribe(handleStart, control);
        subscribe(handleCroupierSample, croupierPort);
        subscribe(handleGradientSample, gradientPort);
        subscribe(handleLeader, leaderPort);
        subscribe(handlePing, networkPort);
        subscribe(handlePong, networkPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            updateLocalNewsView();
        }
    };

    private void updateLocalNewsView() {
        localNewsView = new NewsView(selfAdr.getId(), 0);
        LOG.debug("{}informing overlays of new view", logPrefix);
        trigger(new OverlayViewUpdate.Indication<>(gradientOId, false, localNewsView.copy()), viewUpdatePort);
    }

    Handler handleCroupierSample = new Handler<CroupierSample<NewsView>>() {
        @Override
        public void handle(CroupierSample<NewsView> castSample) {
            croupierSample = castSample;
            sequenceNumber += 1;
            if (!selfAdr.getId().toString().equals("1") || croupierSample.publicSample.isEmpty() || sequenceNumber < 100) {
                return;
            }

            // Print results
            int numberOfNews = newsCoverage.keySet().size();
            if (numberOfNews > 0) {

                double coverageSum = 0;
                for (int newsItem : newsCoverage.keySet()) {
                    int nodes = newsCoverage.get(newsItem).size();
                    double nodesPercent = 100 * nodes / NUMBER_OF_NODES;
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

                System.out.println("\nnumber of news\t" + numberOfNews);
                System.out.println("news coverage\t" + (int) coverageSum / numberOfNews);
                System.out.println("node knowledge\t" + (int) knowledgeSum / NUMBER_OF_NODES);
                System.out.println("for each node\t" + knowledgeList);
            }

            // Disseminate "news"
            newsCoverage.put(sequenceNumber, new HashSet<String>());
            for (Identifier key : croupierSample.publicSample.keySet()) {
                KAddress partner = croupierSample.publicSample.get(key).getSource();
                KHeader header = new BasicHeader(selfAdr, partner, Transport.UDP);
                KContentMsg msg = new BasicContentMsg(header, new Ping(selfAdr, sequenceNumber, TTL));
                trigger(msg, networkPort);
            }
        }
    };

    Handler handleGradientSample = new Handler<TGradientSample>() {
        @Override
        public void handle(TGradientSample sample) {
        }
    };

    Handler handleLeader = new Handler<LeaderUpdate>() {
        @Override
        public void handle(LeaderUpdate event) {
        }
    };

    ClassMatchedHandler handlePing
            = new ClassMatchedHandler<Ping, KContentMsg<?, ?, Ping>>() {

                @Override
                public void handle(Ping content, KContentMsg<?, ?, Ping> container) {
                    LOG.debug("{}received ping from:{}", logPrefix, container.getHeader().getSource().getId());
                    // Forward Ping
                    content.decTTL();
                    if (content.gettTL() > 0) {
                        for (Identifier key : croupierSample.publicSample.keySet()) {
                            KAddress partner = croupierSample.publicSample.get(key).getSource();
                            // don't send back nor to the originator
                            String partnerId = getId(partner);
                            String sourceId  = getId(container.getHeader().getSource());
                            String originId  = getId(content.getOrigin());
                            if (!partnerId.equals(sourceId) && !partnerId.equals(originId)) {
                                KHeader pingHeader = new BasicHeader(selfAdr, partner, Transport.UDP);
                                KContentMsg pingMsg = new BasicContentMsg(pingHeader, content);
                                trigger(pingMsg, networkPort);
                            }
                        }
                    }
                    // Send Pong
                    KHeader pongHeader = new BasicHeader(selfAdr, content.getOrigin(), Transport.UDP);
                    KContentMsg pongMsg = new BasicContentMsg(pongHeader, new Pong(content.getSeqNum()));
                    trigger(pongMsg, networkPort);
                }
            };

    ClassMatchedHandler handlePong
            = new ClassMatchedHandler<Pong, KContentMsg<?, KHeader<?>, Pong>>() {

                @Override
                public void handle(Pong content, KContentMsg<?, KHeader<?>, Pong> container) {
                    LOG.debug("{}received pong from:{}", logPrefix, container.getHeader().getSource().getId());
                    String sourceId = getId(container.getHeader().getSource());
                    newsCoverage.get(content.getSeqNum()).add(sourceId);
                    if (nodeKnowledge.get(sourceId) == null) {
                        nodeKnowledge.put(sourceId, new HashSet<Integer>());
                    }
                    nodeKnowledge.get(sourceId).add(content.getSeqNum());
                }
            };

    private String getId(KAddress kAddress) {
        return kAddress.getId().toString();
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
