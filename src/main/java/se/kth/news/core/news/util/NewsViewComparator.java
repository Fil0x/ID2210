/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * KompicsToolbox is free software; you can redistribute it and/or
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
package se.kth.news.core.news.util;

import com.google.common.primitives.Ints;
import java.util.Comparator;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NewsViewComparator implements Comparator<NewsView> {
    @Override
    public int compare(NewsView o1, NewsView o2) {
        if(o1 == o2) {
            return 0;
        }
        if(o1 == null || o2 == null) {
            throw new NullPointerException();
        }
        if(o1.localNewsCount != o2.localNewsCount) {
            return Ints.compare(o1.localNewsCount, o2.localNewsCount);
        }
        return o1.nodeId.compareTo(o2.nodeId);
    }
}
