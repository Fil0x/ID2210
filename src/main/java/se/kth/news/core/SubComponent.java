package se.kth.news.core;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;

import java.util.Set;

public abstract class SubComponent extends ComponentDefinition {

    //*******************************CONNECTIONS********************************
    protected Positive<Timer> timerPort = requires(Timer.class);
    protected Positive<Network> networkPort = requires(Network.class);
    //*******************************EXTERNAL_STATE*****************************
    protected KAddress selfAdr;

    protected void triggerSend(KAddress node, Object content) {
        KHeader header = new BasicHeader(selfAdr, node, Transport.UDP);
        KContentMsg msg = new BasicContentMsg(header, content);
        trigger(msg, networkPort);
    }

    protected void triggerBroadcast(Set<KAddress> nodes, Object content) {
        for (KAddress n : nodes) {
            triggerSend(n, content);
        }
    }
}
