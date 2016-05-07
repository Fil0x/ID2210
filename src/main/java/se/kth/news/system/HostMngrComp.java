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
package se.kth.news.system;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.news.core.AppMngrComp;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.cc.heartbeat.CCHeartbeatPort;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.omngr.bootstrap.BootstrapClientComp;
import se.sics.ktoolbox.overlaymngr.OverlayMngrComp;
import se.sics.ktoolbox.overlaymngr.OverlayMngrPort;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.nat.NatAwareAddress;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdate;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class HostMngrComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(HostMngrComp.class);
    private String logPrefix = " ";

    //*****************************CONNECTIONS**********************************
    Positive<Timer> timerPort = requires(Timer.class);
    Positive<Network> networkPort = requires(Network.class);
    //***************************EXTERNAL_STATE*********************************
    private KAddress selfAdr;
    private KAddress bootstrapServer;
    private Identifier overlayId;
    //***************************INTERNAL_STATE*********************************
    private Component bootstrapClientComp;
    private Component overlayMngrComp;
    private Component appMngrComp;

    public HostMngrComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.debug("{}initiating...", logPrefix);

        bootstrapServer = init.bootstrapServer;
        overlayId = init.overlayId;

        subscribe(handleStart, control);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.debug("{}starting...", logPrefix);
            connectBootstrapClient();
            connectOverlayMngr();
            connectApp();

            trigger(Start.event, bootstrapClientComp.control());
            trigger(Start.event, overlayMngrComp.control());
            trigger(Start.event, appMngrComp.control());
        }
    };

    private void connectBootstrapClient() {
        bootstrapClientComp = create(BootstrapClientComp.class, new BootstrapClientComp.Init(selfAdr, bootstrapServer));
        connect(bootstrapClientComp.getNegative(Timer.class), timerPort, Channel.TWO_WAY);
        connect(bootstrapClientComp.getNegative(Network.class), networkPort, Channel.TWO_WAY);
    }

    private void connectOverlayMngr() {
        OverlayMngrComp.ExtPort extPorts = new OverlayMngrComp.ExtPort(timerPort, networkPort,
                bootstrapClientComp.getPositive(CCHeartbeatPort.class));
        overlayMngrComp = create(OverlayMngrComp.class, new OverlayMngrComp.Init((NatAwareAddress) selfAdr, extPorts));
    }

    private void connectApp() {
        AppMngrComp.ExtPort extPorts = new AppMngrComp.ExtPort(timerPort, networkPort,
                overlayMngrComp.getPositive(CroupierPort.class), overlayMngrComp.getPositive(GradientPort.class),
                overlayMngrComp.getNegative(OverlayViewUpdatePort.class));
        appMngrComp = create(AppMngrComp.class, new AppMngrComp.Init(extPorts, selfAdr, overlayId));
        connect(appMngrComp.getNegative(OverlayMngrPort.class), overlayMngrComp.getPositive(OverlayMngrPort.class), Channel.TWO_WAY);
    }

    public static class Init extends se.sics.kompics.Init<HostMngrComp> {

        public final KAddress selfAdr;
        public final KAddress bootstrapServer;
        public final Identifier overlayId;

        public Init(KAddress selfAdr, KAddress bootstrapServer, Identifier overlayId) {
            this.selfAdr = selfAdr;
            this.bootstrapServer = bootstrapServer;
            this.overlayId = overlayId;
        }
    }
}
