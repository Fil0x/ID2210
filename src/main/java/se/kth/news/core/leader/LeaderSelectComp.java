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
import se.kth.news.core.Utils;
import se.kth.news.core.news.util.NewsView;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.gradient.event.TGradientSample;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.other.Container;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LeaderSelectComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderSelectComp.class);
    private String logPrefix = " ";

    //*******************************CONNECTIONS********************************
    Positive<Timer> timerPort = requires(Timer.class);
    Positive<Network> networkPort = requires(Network.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Negative<LeaderSelectPort> leaderPort = provides(LeaderSelectPort.class);
    //*******************************EXTERNAL_STATE*****************************
    private KAddress selfAdr;
    //*******************************INTERNAL_STATE*****************************
    private Comparator viewComparator;
    private NewsView selfView;
    private List<Container<KAddress, NewsView>> acquaintances;
    private int sequenceNumber = 0;
    private KAddress leaderAdr;

    private int sessionId = -1;
    private Set<KAddress> unconfirmed;

    public LeaderSelectComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.debug("{}initiating...", logPrefix);

        viewComparator = init.viewComparator;

        subscribe(handleStart, control);
        subscribe(handleGradientSample, gradientPort);
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
            acquaintances = Utils.merge(sample.getGradientFingers(), sample.getGradientNeighbours());

            if (sequenceNumber > 100) {
                if (highestRank()) {
                    if (sequenceNumber == 101) initElection();
                } else {
                    leaderPull();
                }
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
                    LOG.info("{} recived canCommit? from: {}", logPrefix, source.getId());
                    Container<KAddress, NewsView> maxRank = Utils.maxRank(acquaintances);
                    if (maxRank == null || viewComparator.compare(content.body, maxRank.getContent()) >= 0) {
                        triggerSend(source, new Leader2PC(content.sid, "Yes", null));
                    } else {
                        triggerSend(source, new Leader2PC(content.sid, "No", null));
                    }
                    break;
                case "Yes":
                    if (content.sid == sessionId) {
                        unconfirmed.remove(source);
                        if (unconfirmed.isEmpty()) {
                            triggerBroadcast(Utils.addressSet(acquaintances), new Leader2PC(sessionId, "doCommit", selfAdr));
                            trustLeader(selfAdr);
                        }
                    }
                    break;
                case "No":
                    if (content.sid == sessionId) {
                        triggerBroadcast(Utils.addressSet(acquaintances), new Leader2PC(sessionId, "abortCommit", null));
                    }
                    break;
                case "doCommit":
                    LOG.info("{} recived doCommit from: {}", logPrefix, source.getId());
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
            if (leaderAdr != null) {
                KHeader header = new BasicHeader(selfAdr, container.getHeader().getSource(), Transport.UDP);
                KContentMsg msg = new BasicContentMsg(header, new LeaderPush(leaderAdr));
                trigger(msg, networkPort);
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
    private boolean highestRank() {
        Container<KAddress, NewsView> maxRank = Utils.maxRank(acquaintances);
        if (maxRank == null || viewComparator.compare(selfView, maxRank.getContent()) > 0) {
            return true;
        }
        return false;
    }

    private void initElection() {
        sessionId += 1;
        unconfirmed = Utils.addressSet(acquaintances);
        triggerBroadcast(unconfirmed, new Leader2PC(sessionId, "canCommit?", selfView));
    }

    private void trustLeader(KAddress newLeaderAdr) {
        if (leaderAdr == null || !leaderAdr.equals(newLeaderAdr)) {
            leaderAdr = newLeaderAdr;
            trigger(new LeaderUpdate(leaderAdr), leaderPort);
        }
    }

    private void leaderPull() {
        KHeader header = new BasicHeader(selfAdr, Utils.maxRank(acquaintances).getSource(), Transport.UDP);
        KContentMsg msg = new BasicContentMsg(header, new LeaderPull());
        trigger(msg, networkPort);
    }

    private void triggerBroadcast(Set<KAddress> nodes, Object content) {
        for (KAddress n : nodes) {
            triggerSend(n, content);
        }
    }

    private void triggerSend(KAddress node, Object content) {
        KHeader header = new BasicHeader(selfAdr, node, Transport.UDP);
        KContentMsg msg = new BasicContentMsg(header, content);
        trigger(msg, networkPort);
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
