/*
 * Copyright 2018-2022 Volkan Yazıcı
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permits and
 * limitations under the License.
 */

package com.vlkan.rfos.policy;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vlkan.rfos.Clock;
import com.vlkan.rfos.Rotatable;
import com.vlkan.rfos.RotationConfig;

import utils.LocalDateTimes;
import utils.LoggerSettable;

/**
 * Base class for implementing periodically triggered time-based policies.
 *
 * @see DailyRotationPolicy
 * @see WeeklyRotationPolicy
 */
public abstract class TimeBasedRotationPolicy implements RotationPolicy, LoggerSettable {
    private volatile ScheduledFuture<?> scheduledFuture;
    private Logger m_logger = LoggerFactory.getLogger(getClass());

    /**
     * @return {@code false}, always
     */
    @Override
    public boolean isWriteSensitive() {
        return false;
    }

    /**
     * Throws an exception, always, since this is not a write-sensitive policy.
     *
     * @throws UnsupportedOperationException thrown upon every call
     */
    @Override
    public void acceptWrite(long byteCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void start(Rotatable rotatable) {
        RotationConfig config = rotatable.getConfig();
        Clock clock = config.getClock();
        Instant currentInstant = clock.now();
        Instant triggerInstant = getTriggerInstant(clock);
        long triggerDelayMillis = Duration.between(currentInstant, triggerInstant).toMillis();
    	
        getLogger().debug("setup a trigger {triggerInstant={}}", LocalDateTimes.fromInstant(triggerInstant));
        Runnable task = createTask(rotatable, triggerInstant);
        this.scheduledFuture = config.getExecutorService()
                					.schedule(task, triggerDelayMillis, TimeUnit.MILLISECONDS);
    }

    private Runnable createTask(Rotatable rotatable, Instant triggerInstant) {
        return () -> {
            getLogger().debug("rotation started: now={}", LocalDateTimes.fromInstant(triggerInstant));
            rotatable.rotate(TimeBasedRotationPolicy.this, triggerInstant);
            start(rotatable);
        };
    }

    @Override
    public synchronized void stop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
    }

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = (logger != null) ? logger : LoggerFactory.getLogger(getClass());
	}

    /**
     * @param clock a clock implementation
     *
     * @return the upcoming rotation trigger instant
     */
    abstract public Instant getTriggerInstant(Clock clock);

}
