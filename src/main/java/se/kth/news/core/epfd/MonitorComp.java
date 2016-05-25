package se.kth.news.core.epfd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.news.core.SubComponent;
import se.kth.news.core.Utils;
import se.sics.kompics.*;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class MonitorComp extends SubComponent {

    private static final Logger LOG = LoggerFactory.getLogger(MonitorComp.class);
    private String logPrefix = " ";

    //*******************************CONNECTIONS********************************
    Negative<MonitorPort> monitorPort = provides(MonitorPort.class);
    //*******************************INTERNAL_STATE*****************************
    private static final int DELTA = 10000;
    private UUID lastSetTimer;
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

    //*******************************HANDLERS***********************************
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
            if (monitorTimeout.getTimeoutId() == lastSetTimer) { // check is not 100%!
                if (!Utils.intersection(alive, suspected).isEmpty()) {
                    delay += DELTA;
                }
                for (KAddress p : allNodes) {
                    if (!alive.contains(p) && !suspected.contains(p)) {
                        suspected.add(p);
                        trigger(new Suspect(p), monitorPort);
                    } else if (alive.contains(p) && suspected.contains(p)) {
                        suspected.remove(p);
                        trigger(new Restore(p), monitorPort);
                    }
                    triggerSend(p, new HeartbeatRequest());
                }
                alive = new HashSet<>();
                startTimer(delay);
            }
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

    //*******************************HELP_FUNCTIONS*****************************
    private void startTimer(long delay) {
        ScheduleTimeout st = new ScheduleTimeout(delay);
        MonitorTimeout mt = new MonitorTimeout(st);
        st.setTimeoutEvent(mt);
        trigger(st, timerPort);
        lastSetTimer = mt.getTimeoutId();
    }

    public static class Init extends se.sics.kompics.Init<MonitorComp> {

        public final KAddress selfAdr;

        public Init(KAddress selfAdr) {
            this.selfAdr = selfAdr;
        }
    }
}
