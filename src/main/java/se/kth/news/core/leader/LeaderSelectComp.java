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
import se.kth.news.core.epfd.MonitorRequest;
import se.kth.news.core.epfd.MonitorPort;
import se.kth.news.core.epfd.Restore;
import se.kth.news.core.epfd.Suspect;
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
    Positive<MonitorPort> monitorPort = requires(MonitorPort.class);
    Negative<LeaderSelectPort> leaderPort = provides(LeaderSelectPort.class);
    //*******************************EXTERNAL_STATE*****************************
    private KAddress selfAdr;
    //*******************************INTERNAL_STATE*****************************
    private Comparator viewComparator;
    private NewsView selfView;
    private List<Container<KAddress, NewsView>> fingers;
    private List<Container<KAddress, NewsView>> neighbors;
    private int sequenceNumber = 0;
    private KAddress leaderAdr;

    public LeaderSelectComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.debug("{}initiating...", logPrefix);
        
        viewComparator = init.viewComparator;

        subscribe(handleStart, control);
        subscribe(handleGradientSample, gradientPort);
        subscribe(handleLeaderPull, networkPort);
        subscribe(handleLeaderPush, networkPort);
        subscribe(handleRestore, monitorPort);
        subscribe(handleSuspect, monitorPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.debug("{}starting...", logPrefix);
        }
    };
    
    Handler handleGradientSample = new Handler<TGradientSample>() {
        @Override
        public void handle(TGradientSample sample) {
            LOG.debug("{}neighbours:{}", logPrefix, sample.gradientNeighbours);
            LOG.debug("{}fingers:{}", logPrefix, sample.gradientFingers);
            LOG.debug("{}local view:{}", logPrefix, sample.selfView);

            selfView = (NewsView) sample.selfView;
            fingers = sample.getGradientFingers();
            neighbors = sample.getGradientNeighbours();

            if (sequenceNumber++ > 100) {
                if (iAmTheLeader()) {
                    setLeader(selfAdr);
                } else {
                    if (leaderAdr != null && leaderAdr.equals(selfAdr)) leaderAdr = null;
                    leaderPull();
                }
            }
        }
    };

    private boolean iAmTheLeader() {
        if (viewComparator.compare(selfView, getHighestFinger().getContent()) < 0) {
            return false;
        } else {
            return true;
        }
    }

    private void setLeader(KAddress leaderAdr_) {
        if (leaderAdr == null || !leaderAdr.equals(leaderAdr_)) {
            trigger(new LeaderUpdate(leaderAdr = leaderAdr_), leaderPort);
            if (hasLeaderNeighbor()) {
                Set<KAddress> nodesToMonitor = getAddressSet(neighbors);
                trigger(new MonitorRequest(nodesToMonitor), monitorPort);
                LOG.info("{}requested monitoring:{}", logPrefix, nodesToMonitor);
                //if (!hasLeaderFinger()) System.out.println("WTF");
            }
        }
    }

    private Set<KAddress> getAddressSet(List<Container<KAddress, NewsView>> containerList) {
        HashSet<KAddress> addressSet = new HashSet<>();
        for (Container<KAddress, NewsView> c : containerList) {
            addressSet.add(c.getSource());
        }
        return addressSet;
    }

    private boolean hasLeaderNeighbor() {
        for (int i = 0; i < neighbors.size(); i++) {
            if (neighbors.get(i).getSource().equals(leaderAdr)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasLeaderFinger() {
        for (int i = 0; i < fingers.size(); i++) {
            if (fingers.get(i).getSource().equals(leaderAdr)) {
                return true;
            }
        }
        return false;
    }

    private void leaderPull() {
        Container<KAddress, NewsView> highestFinger = getHighestFinger();
        KHeader header = new BasicHeader(selfAdr, highestFinger.getSource(), Transport.UDP);
        KContentMsg msg = new BasicContentMsg(header, new LeaderPull());
        trigger(msg, networkPort);
    }

    private Container<KAddress, NewsView> getHighestFinger() {
        Container<KAddress, NewsView> highestFinger = fingers.get(0);
        for (int i = 1; i < fingers.size(); i++) {
            if (viewComparator.compare(highestFinger.getContent(), fingers.get(i).getContent()) < 0) {
                highestFinger = fingers.get(i);
            }
        }
        return highestFinger;
    }

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
            setLeader(container.getContent().leaderAdr);
        }
    };

    Handler handleSuspect = new Handler<Suspect>() {
        @Override
        public void handle(Suspect event) {
            LOG.info("{}", "suspect");
        }
    };

    Handler handleRestore = new Handler<Restore>() {
        @Override
        public void handle(Restore event) {
            LOG.info("{}", "restore");
        }
    };

    public static class Init extends se.sics.kompics.Init<LeaderSelectComp> {

        public final KAddress selfAdr;
        public final Comparator viewComparator;

        public Init(KAddress selfAdr, Comparator viewComparator) {
            this.selfAdr = selfAdr;
            this.viewComparator = viewComparator;
        }
    }
}
