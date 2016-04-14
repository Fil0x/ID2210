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
package se.kth.news.sim;

import java.net.InetAddress;
import java.net.UnknownHostException;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.identifiable.basic.IntIdentifier;
import se.sics.ktoolbox.util.identifiable.basic.OverlayIdFactory;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicAddress;
import se.sics.ktoolbox.util.network.nat.NatAwareAddressImpl;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class ScenarioSetup {

    public static final long scenarioSeed = 1234;
    public static final int appPort = 12345;
    public static final KAddress bootstrapServer;
    
    public static byte overlayOwner = 0x10;
    public static final Identifier newsOverlayId;

    static {
        try {
            bootstrapServer = NatAwareAddressImpl.open(new BasicAddress(InetAddress.getByName("193.0.0.1"), appPort, new IntIdentifier(0)));
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
        newsOverlayId = OverlayIdFactory.getId(overlayOwner, OverlayIdFactory.Type.TGRADIENT, new byte[]{0, 0, 1});
    }

    public static KAddress getNodeAdr(int nodeId) {
        try {
            return NatAwareAddressImpl.open(new BasicAddress(InetAddress.getByName("193.0.0." + nodeId), appPort, new IntIdentifier(nodeId)));
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }
    public static long getNodeSeed(int nodeId) {
        return scenarioSeed + nodeId;
    }
}
