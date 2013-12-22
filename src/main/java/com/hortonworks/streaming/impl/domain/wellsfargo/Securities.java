package com.hortonworks.streaming.impl.domain.wellsfargo;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;

import akka.actor.ActorRef;

import com.hortonworks.streaming.impl.domain.AbstractEventEmitter;
import com.hortonworks.streaming.impl.domain.Event;
import com.hortonworks.streaming.impl.messages.EmitEvent;

public class Securities extends AbstractEventEmitter {

	private static final long serialVersionUID = -1562734450569520172L;
    private Random random = new Random();
    private static long messageCount = 0; 
    private static long started = new Date().getTime();

	public Securities() {
	}

	public Event generateEvent() {
        // select which event to generate
        messageCount++;
        
        if ( (messageCount % 1000) == 0 )  System.err.println( "message " + String.valueOf(messageCount) + ". " + String.valueOf( messageCount / ((new Date().getTime() - started) / 1000)) + " msg/s"); 
        int selected = random.nextInt( 8 ) + 1;


//        return new FixEvent( FixEvent.FixEventEnum.fix1 );
        
        // help: how to organize this properly ??
        if ( selected == 1 ) return new SdrEvent( SdrEvent.SdrEventEnum.sdr_4k);
        else if ( selected == 2 ) return new SdrEvent( SdrEvent.SdrEventEnum.sdr_16k);
        else if ( selected == 3 ) return new SdrEvent( SdrEvent.SdrEventEnum.sdr_240k);
        else if ( selected == 4 ) return new CommoditiesEvent();
        else if ( selected == 5 ) return new ScritturaEvent();
        else if ( selected == 6 ) return new FixEvent( FixEvent.FixEventEnum.fix1 );
        else if ( selected == 7 ) return new FixEvent( FixEvent.FixEventEnum.fix2 );
        else if ( selected == 8 ) return new FixEvent( FixEvent.FixEventEnum.fix3 );
        else throw new RuntimeException("Invalid even selected.");

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
