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

import java.time.Instant;

import com.vlkan.rfos.Clock;

/**
 * Policy for triggering a rotation at Sunday midnight every day.
 */
public class WeeklyRotationPolicy extends TimeBasedRotationPolicy {
    private static final WeeklyRotationPolicy INSTANCE = new WeeklyRotationPolicy();

    private WeeklyRotationPolicy() {
        // Do nothing.
    }

    /**
     * @return an instance of this policy
     */
    public static WeeklyRotationPolicy getInstance() {
        return INSTANCE;
    }

    /**
     * @return the instant of the upcoming Sunday midnight
     */
    @Override
    public Instant getTriggerInstant(Clock clock) {
        return clock.sundayMidnight();
    }

    @Override
    public String toString() {
        return "WeeklyRotationPolicy";
    }

}
