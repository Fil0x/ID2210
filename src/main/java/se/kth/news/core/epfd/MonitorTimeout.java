package se.kth.news.core.epfd;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

public class MonitorTimeout extends Timeout {

    protected MonitorTimeout(ScheduleTimeout st) {
        super(st);
    }
}
