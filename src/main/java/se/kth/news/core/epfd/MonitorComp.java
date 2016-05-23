package se.kth.news.core.epfd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;

public class MonitorComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(MonitorComp.class);

    //*******************************CONNECTIONS********************************
    Positive<Timer> timerPort = requires(Timer.class);
    Positive<Network> networkPort = requires(Network.class);
    Negative<MonitorPort> monitorPort = provides(MonitorPort.class);

    Handler handleMonitorRequest = new Handler<MonitorRequest>() {
        @Override
        public void handle(MonitorRequest event) {
            LOG.info("{}", "!!!!!!!");
        }
    };

    {
        subscribe(handleMonitorRequest, monitorPort);
    }
}
