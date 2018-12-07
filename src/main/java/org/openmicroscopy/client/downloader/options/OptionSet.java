/*
 * Copyright (C) 2016-2018 University of Dundee & Open Microscopy Environment.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

package org.openmicroscopy.client.downloader.options;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A set of command-line options of which a subset may be chosen.
 * @author m.t.b.carroll@dundee.ac.uk
 */
class OptionSet {

    /** the string representing <q>none of the options</q>, can be used to override defaults
     */
    static final String NONE = "none";

    /**
     * The chosen subset of command-line options.
     * @author m.t.b.carroll@dundee.ac.uk
     */
    class Chosen {

        private final ImmutableMap<String, Boolean> options;

        /**
         * Check if the given option was chosen.
         * @param option an option
         * @return if it was chosen
         */
        boolean isOptionSet(String option) {
            return options.get(option);
        }

        /**
         * Specify which options were chosen.
         * @param choices the user's choices, both positive and negative
         */
        private Chosen(Iterable<Map.Entry<String, Boolean>> choices) {
            final ImmutableMap.Builder<String, Boolean> builder = ImmutableMap.builder();
            for (final Map.Entry<String, Boolean> choice : choices) {
                builder.put(choice);
            }
            options = builder.build();
        }
    }

    private final ImmutableSortedSet<String> options, defaults;
    private final Set<String> hidden = new HashSet<>();

    /**
     * Construct a set of command-line options.
     * @param options the full set of options
     * @param defaults the default subset of options
     */
    OptionSet(Iterable<String> options, Iterable<String> defaults) {
        this.options = ImmutableSortedSet.copyOf(options);
        this.defaults = ImmutableSortedSet.copyOf(defaults);
        if (!Sets.difference(this.defaults, this.options).isEmpty()) {
            throw new IllegalArgumentException("default options must be subset of all available");
        }
    }

    /**
     * Prevent {@link #getOptionNames()} and {@link #getDefaultNames()} from returning the given option.
     * @param optionToHide one of the options with which this instance was constructed
     * @return this instance, for method chaining
     */
    OptionSet hide(String optionToHide) {
        if (options.contains(optionToHide)) {
            hidden.add(optionToHide);
        } else {
            throw new IllegalArgumentException("unknown option: " + optionToHide);
        }
        return this;
    }

    /**
     * Get the names of the options.
     * @return the options, sorted
     */
    Collection<String> getOptionNames() {
        return Sets.difference(options, hidden);
    }

    /**
     * Get the names of the default options, may be empty.
     * @return the defaults, sorted
     */
    Collection<String> getDefaultNames() {
        return Sets.difference(defaults, hidden);
    }

    /**
     * Parse a comma-separated list of option names, resorting to defaults if need be.
     * @param choices comma-separated option names, never {@code null}
     * @return the parsed choices
     */
    Chosen parse(String choices) {
        final Map<String, Boolean> chosen = new HashMap<>();
        for (final String option : options) {
            chosen.put(option, false);
        }
        ImmutableSet<String> eachChoice = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(choices));
        if (eachChoice.isEmpty()) {
            eachChoice = defaults;
        }
        for (final String option : Sets.difference(eachChoice, Collections.singleton(NONE))) {
            if (options.contains(option)) {
                chosen.put(option, true);
            } else {
                throw new IllegalArgumentException("unknown option: " + option);
            }
        }
        return new Chosen(chosen.entrySet());
    }
}
