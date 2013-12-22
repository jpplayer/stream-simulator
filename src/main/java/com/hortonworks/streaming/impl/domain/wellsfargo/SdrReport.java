package com.hortonworks.streaming.impl.domain.wellsfargo;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;

import akka.actor.ActorRef;

import com.hortonworks.streaming.impl.domain.AbstractEventEmitter;
import com.hortonworks.streaming.impl.messages.EmitEvent;

public class SdrReport extends AbstractEventEmitter {

	private static final long serialVersionUID = -1562734450569520172L;

	public SdrReport() {
	}


	public SdrEvent generateEvent() {
        return new SdrEvent();
	}


	@Override
	public String toString() {
		return new String( String.valueOf( new Date().getTime()) ) ;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof EmitEvent) {
			ActorRef actor = this.context().system()
					.actorFor("akka://EventSimulator/user/eventCollector");
			Random rand = new Random();
			int sleepOffset = rand.nextInt(200);
			while (true) {
//				Thread.sleep(500 + sleepOffset);
				Thread.sleep(1); // 1000 events per second
				actor.tell(generateEvent(), this.getSender());
			}
		}
	}
}
