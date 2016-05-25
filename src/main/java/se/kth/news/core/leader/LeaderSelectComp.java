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
package se.kth.news.core.leader;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.news.core.SubComponent;
import se.kth.news.core.Utils;
import se.kth.news.core.epfd.MonitorRequest;
import se.kth.news.core.epfd.MonitorPort;
import se.kth.news.core.epfd.Restore;
import se.kth.news.core.epfd.Suspect;
import se.kth.news.core.news.util.NewsView;
import se.sics.kompics.*;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.gradient.event.TGradientSample;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.other.Container;

public class LeaderSelectComp extends SubComponent {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderSelectComp.class);
    private String logPrefix = " ";

    //*******************************CONNECTIONS********************************
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Positive<MonitorPort> monitorPort = requires(MonitorPort.class);
    Negative<LeaderSelectPort> leaderPort = provides(LeaderSelectPort.class);
    //*******************************INTERNAL_STATE*****************************
    private Comparator viewComparator;
    private NewsView selfView;
    private List<Container<KAddress, NewsView>> fingers;
    private int sequenceNumber = 0;
    private int sessionId = -1;
    private KAddress leaderAdr;
    private Set<KAddress> suspected = new HashSet<>();
    private Set<KAddress> unconfirmed;

    public LeaderSelectComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.debug("{}initiating...", logPrefix);

        viewComparator = init.viewComparator;

        subscribe(handleStart, control);
        subscribe(handleGradientSample, gradientPort);
        subscribe(handleSuspect, monitorPort);
        subscribe(handleRestore, monitorPort);
        subscribe(handleLeader2PC, networkPort);
        subscribe(handleLeaderPull, networkPort);
        subscribe(handleLeaderPush, networkPort);
    }

    //*******************************HANDLERS***********************************
    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.debug("{}starting...", logPrefix);
        }
    };

    Handler handleGradientSample = new Handler<TGradientSample>() {
        @Override
        public void handle(TGradientSample sample) {
            sequenceNumber += 1;

            LOG.debug("{}neighbours:{}", logPrefix, sample.gradientNeighbours);
            LOG.debug("{}fingers:{}", logPrefix, sample.gradientFingers);
            LOG.debug("{}local view:{}", logPrefix, sample.selfView);

            selfView = (NewsView) sample.selfView;
            fingers = sample.getGradientFingers();

            if (sequenceNumber > 100) {
                updateMonitor();

                if (highestRank()) {
                    initElection();
                } else {
                    leaderPull();
                }
            }
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

    ClassMatchedHandler handleLeader2PC
            = new ClassMatchedHandler<Leader2PC, KContentMsg<?, ?, Leader2PC>>() {
        @Override
        public void handle(Leader2PC content, KContentMsg<?, ?, Leader2PC> container) {
            KAddress source = container.getHeader().getSource();
            switch (content.header) {
                case "canCommit?":
                    Container<KAddress, NewsView> maxRank = Utils.maxRank(fingers, suspected);
                    if (maxRank == null || viewComparator.compare(content.body, maxRank.getContent()) >= 0) {
                        triggerSend(source, new Leader2PC(content.sid, "Yes", null));
                    }
                    else {
                        triggerSend(source, new Leader2PC(content.sid, "No", null));
                    }
                    break;
                case "Yes":
                    if (content.sid == sessionId) {
                        unconfirmed.remove(source);
                        if (unconfirmed.isEmpty()) {
                            triggerBroadcast(Utils.addressSet(fingers), new Leader2PC(sessionId, "doCommit", selfAdr));
                            trustLeader(selfAdr);
                        }
                    }
                    break;
                case "No":
                    if (content.sid == sessionId) {
                        triggerBroadcast(Utils.addressSet(fingers), new Leader2PC(sessionId, "abortCommit", null));
                    }
                    break;
                case "doCommit":
                    trustLeader((KAddress) content.body);
                    break;
                /*case "abortCommit":
                    break;
                case "haveCommited":
                    break;
                default:
                    break;*/
            }
        }
    };

    ClassMatchedHandler handleLeaderPull
            = new ClassMatchedHandler<LeaderPull, KContentMsg<?, ?, LeaderPull>>() {
        @Override
        public void handle(LeaderPull content, KContentMsg<?, ?, LeaderPull> container) {
            KAddress source = container.getHeader().getSource();
            if (leaderAdr != null) {
                triggerSend(source, new LeaderPush(leaderAdr));
            }
        }
    };

    ClassMatchedHandler handleLeaderPush
            = new ClassMatchedHandler<LeaderPush, KContentMsg<?, ?, LeaderPush>>() {
        @Override
        public void handle(LeaderPush content, KContentMsg<?, ?, LeaderPush> container) {
            trustLeader(container.getContent().leaderAdr);
        }
    };

    //*******************************HELP_FUNCTIONS*****************************
    private void updateMonitor() {
        Set<KAddress> nodeToMonitor = new HashSet<>(Utils.addressSet(fingers));
        if (leaderAdr != null) nodeToMonitor.add(leaderAdr);
        trigger(new MonitorRequest(nodeToMonitor), monitorPort);
    }


    private boolean highestRank() {
        Container<KAddress, NewsView> maxRank = Utils.maxRank(fingers, suspected);
        if (maxRank == null || viewComparator.compare(selfView, maxRank.getContent()) > 0) {
            return true;
        }
        return false;
    }

    private void initElection() {
        sessionId += 1;
        unconfirmed = Utils.addressSet(fingers);
        unconfirmed.removeAll(suspected);
        triggerBroadcast(unconfirmed, new Leader2PC(sessionId, "canCommit?", selfView));
    }

    private void trustLeader(KAddress newLeaderAdr) {
        if (leaderAdr == null || !leaderAdr.equals(newLeaderAdr)) {
            leaderAdr = newLeaderAdr;
            trigger(new LeaderUpdate(leaderAdr), leaderPort);
        }
    }

    private void leaderPull() {
        triggerSend(Utils.maxRank(fingers).getSource(), new LeaderPull());
    }


    public static class Init extends se.sics.kompics.Init<LeaderSelectComp> {

        public final KAddress selfAdr;
        public final Comparator viewComparator;

        public Init(KAddress selfAdr, Comparator viewComparator) {
            this.selfAdr = selfAdr;
            this.viewComparator = viewComparator;
        }
    }
}
