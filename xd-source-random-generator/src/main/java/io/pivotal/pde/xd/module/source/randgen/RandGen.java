package io.pivotal.pde.xd.module.source.randgen;

import java.util.Date;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.springframework.format.datetime.DateFormatter;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.xd.tuple.Tuple;
import org.springframework.xd.tuple.TupleBuilder;


public class RandGen extends MessageProducerSupport{
	private final AtomicBoolean running = new AtomicBoolean(false);

	private RandomDataGenerator tempRandom = new RandomDataGenerator();
	private RandomDataGenerator presRandom = new RandomDataGenerator();

	private final ExecutorService executorService = Executors.newSingleThreadExecutor(new CustomizableThreadFactory(
			"randsimu"));  
	@Override
	protected void doStart() {
		if (running.compareAndSet(false, true)) {
			executorService.submit(new RandSimuExecutor());
		}
	}

	@Override
	protected void doStop() {
		if (running.compareAndSet(true, false)) {
			executorService.shutdown();
		}
	}

	class RandSimuExecutor implements Runnable {

		public void run() {
			DateFormatter dateFormatter = new DateFormatter("yyyy-MM-dd HH:mm:ss");

			while (running.get()) {
				Tuple tuple = TupleBuilder.tuple()
						.put("timestamp", dateFormatter.print(new Date(), Locale.US))
						.put("pressure",tempRandom.nextGaussian(170.0,25.0))
						.put("temporature", presRandom.nextGaussian(900.0, 75.0))
						.build();

				sendMessage(MessageBuilder.withPayload(tuple).build());

				try {
					Thread.sleep(1000);
				}
				catch (InterruptedException e) {

					running.set(false);
				}
			}

		}
	}
}
