package se.kth.news.core.epfd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class MonitorComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(MonitorComp.class);
    private String logPrefix = " ";

    //*******************************CONNECTIONS********************************
    Positive<Timer> timerPort = requires(Timer.class);
    Positive<Network> networkPort = requires(Network.class);
    Negative<MonitorPort> monitorPort = provides(MonitorPort.class);
    //*******************************EXTERNAL_STATE*****************************
    private KAddress selfAdr;
    //*******************************INTERNAL_STATE*****************************
    private static final int DELTA = 10000;
    private Set<KAddress> allNodes;
    private Set<KAddress> alive;
    private Set<KAddress> suspected;
    private int delay;

    public MonitorComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.debug("{}initiating...", logPrefix);

        subscribe(handleStart, control);
        subscribe(handleMonitorRequest, monitorPort);
        subscribe(handleMonitorTimeout, timerPort);
        subscribe(handleHeartbeatRequest, networkPort);
        subscribe(handleHeartbeatReply, networkPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.debug("{}starting...", logPrefix);
        }
    };

    Handler handleMonitorRequest = new Handler<MonitorRequest>() {
        @Override
        public void handle(MonitorRequest event) {
            allNodes = event.nodes;
            alive = new HashSet<>(allNodes); // deep copy
            suspected = new HashSet<>();
            delay = DELTA;
            startTimer(delay);
        }
    };

    Handler<MonitorTimeout> handleMonitorTimeout = new Handler<MonitorTimeout>() {
        @Override
        public void handle(MonitorTimeout monitorTimeout) {
            if (!getIntersection(alive, suspected).isEmpty()) {
                delay += DELTA;
            }
            for (KAddress p : allNodes) {
                if (!alive.contains(p) && !suspected.contains(p)) {
                    suspected.add(p);
                    trigger(new Suspect(p), monitorPort);
                }
                else if (alive.contains(p) && suspected.contains(p)) {
                    suspected.remove(p);
                    trigger(new Restore(p), monitorPort);
                }
                triggerSend(p, new HeartbeatRequest());
            }
            alive = new HashSet<>();
            startTimer(delay);
        }
    };

    ClassMatchedHandler handleHeartbeatRequest
            = new ClassMatchedHandler<HeartbeatRequest, KContentMsg<?, ?, HeartbeatRequest>>() {
        @Override
        public void handle(HeartbeatRequest content, KContentMsg<?, ?, HeartbeatRequest> container) {
            KAddress q = container.getHeader().getSource();
            triggerSend(q, new HeartbeatReply());
        }
    };

    ClassMatchedHandler handleHeartbeatReply
            = new ClassMatchedHandler<HeartbeatReply, KContentMsg<?, ?, HeartbeatReply>>() {
        @Override
        public void handle(HeartbeatReply content, KContentMsg<?, ?, HeartbeatReply> container) {
            KAddress p = container.getHeader().getSource();
            alive.add(p);
        }
    };

    private void startTimer(long delay) {
        ScheduleTimeout st = new ScheduleTimeout(delay);
        st.setTimeoutEvent(new MonitorTimeout(st));
        trigger(st, timerPort);
    }

    private Set<KAddress> getIntersection(Set<KAddress> s1, Set<KAddress> s2) {
        Set<KAddress> intersection = new HashSet<>(s1);
        intersection.retainAll(s2);
        return intersection;
    }

    private void triggerSend(KAddress node, Serializable content) {
        KHeader header = new BasicHeader(selfAdr, node, Transport.UDP);
        KContentMsg msg = new BasicContentMsg(header, content);
        trigger(msg, networkPort);
    }

    private class MonitorTimeout extends Timeout {

        MonitorTimeout(ScheduleTimeout st) {
            super(st);
        }
    }

    public static class Init extends se.sics.kompics.Init<MonitorComp> {

        public final KAddress selfAdr;

        public Init(KAddress selfAdr) {
            this.selfAdr = selfAdr;
        }
    }
}
