/**
 * Copyright (c) 2010-2021 Contributors to the openHAB project
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.openhab.binding.balboa.internal;

import java.util.HashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.measure.quantity.Temperature;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import org.openhab.binding.balboa.internal.BalboaMessage.ItemType;
import org.openhab.binding.balboa.internal.BalboaMessage.PanelConfigurationResponseMessage;
import org.openhab.binding.balboa.internal.BalboaMessage.SettingsRequestMessage.SettingsType;
import org.openhab.binding.balboa.internal.BalboaProtocol.Handler;
import org.openhab.binding.balboa.internal.BalboaProtocol.Status;
import org.openhab.core.library.types.OnOffType;
import org.openhab.core.library.types.OpenClosedType;
import org.openhab.core.library.types.QuantityType;
import org.openhab.core.library.types.StringType;
import org.openhab.core.library.unit.ImperialUnits;
import org.openhab.core.library.unit.SIUnits;
import org.openhab.core.thing.Channel;
import org.openhab.core.thing.ChannelUID;
import org.openhab.core.thing.Thing;
import org.openhab.core.thing.ThingStatus;
import org.openhab.core.thing.ThingStatusDetail;
import org.openhab.core.thing.binding.BaseThingHandler;
import org.openhab.core.thing.binding.builder.ChannelBuilder;
import org.openhab.core.thing.binding.builder.ThingBuilder;
import org.openhab.core.thing.type.ChannelTypeUID;
import org.openhab.core.types.Command;
import org.openhab.core.types.RefreshType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BalboaHandler} is responsible for handling Balboa things.
 *
 * @author Carl Önnheim - Initial contribution
 */
@NonNullByDefault
public class BalboaHandler extends BaseThingHandler implements Handler {

    // Logger and configuration fields
    private final Logger logger = LoggerFactory.getLogger(BalboaHandler.class);
    private BalboaConfiguration config = getConfigAs(BalboaConfiguration.class);

    // Instantiate a protocol with this instance as the handler (receiving callbacks)
    private BalboaProtocol protocol = new BalboaProtocol(this);
    // This manages the reconnection attempts
    private ReconnectJob reconnectJob = new ReconnectJob();
    // This is used to poll the unit for non-broadcast information and keeping the connection alive.
    private PollingJob pollingJob = new PollingJob();

    /**
     * Helper class providing a thread safe reconnection mechanism.
     *
     * @author CarlÖnnheim
     *
     */
    private class ReconnectJob implements Runnable {
        private @Nullable ScheduledFuture<?> job;
        private boolean enabled = false;

        /**
         * Enables the reconnection mechanism
         */
        public void enable() {
            // Store the state
            enabled = true;
        }

        /**
         * Disables the reconnection mechanism
         */
        public void disable() {
            // Cancel pending reconnects if disabled
            if (job != null) {
                job.cancel(true);
                job = null;
            }

            // Store the state
            enabled = false;
        }

        /**
         * Schedules a reconnect if enabled and not already pending.
         */
        public synchronized void schedule() {
            if (enabled && job == null && config.reconnectInterval > 0) {
                job = scheduler.schedule(this, config.reconnectInterval, TimeUnit.SECONDS);
                logger.debug("Reconnection attempt in {} seconds", config.reconnectInterval);
            }
        }

        /**
         * Performs the reconnect and marks the job as not pending
         */
        @Override
        public synchronized void run() {
            // Clear the job first since a new reconnect may be scheduled by the connect call
            job = null;
            logger.debug("Reconnecting...");
            connect();
        }
    }

    /**
     * Helper class providing a thread safe polling mechanism.
     *
     * @author CarlÖnnheim
     *
     */
    private class PollingJob implements Runnable {
        private @Nullable ScheduledFuture<?> job;

        /**
         * Starts polling if not already active.
         */
        public synchronized void start() {
            if (job == null && config.pollingInterval > 0) {
                job = scheduler.scheduleWithFixedDelay(this, config.pollingInterval, config.pollingInterval,
                        TimeUnit.SECONDS);
                logger.debug("Polling started with {} seconds interval", config.pollingInterval);
            }
        }

        /**
         * Stops polling if active.
         */
        public synchronized void stop() {
            if (job != null) {
                job.cancel(true);
                logger.debug("Polling stopped");
                job = null;
            }
        }

        /**
         * Sends a polling packet
         */
        @Override
        public void run() {
            logger.trace("Polling the unit");
            // Send an information request
            protocol.sendMessage(new BalboaMessage.SettingsRequestMessage(SettingsType.INFORMATION));
        }
    }

    // We keep all channels in a hash map. It is easier to treat all channels the same way, since the majority are
    // dynamic.
    private class ChannelMap extends HashMap<ChannelUID, BalboaChannel> {
        private static final long serialVersionUID = 1L;

        // Helper method to add a channel with its UID as key
        protected void addChannel(BalboaChannel channel) {
            put(channel.getChannelUID(), channel);
        }
    }

    private ChannelMap channels = new ChannelMap();

    /*
     * Basic structure of the handler implementation is as follows
     *
     * Block A - Basic handler functions. Deals with instatiation, intitialize/dispose and handling of commands
     * Block B - Handling events from the communication protocol (status updates and messages).
     * Block C - Passing updates to/from the framework from/to the balboa unit.
     *
     * Block A begins here
     *
     */

    /**
     * Constructs a {@link BalboaHandler} for a thing.
     *
     * @param thing
     */
    public BalboaHandler(Thing thing) {
        super(thing);
    }

    /**
     * Initializes a {@link BalboaHandler}. The configuration is reread and the communication protocol with the unit is
     * connected.
     *
     */
    @Override
    public void initialize() {

        // Initialize the status as UNKNOWN. The protocol will update the status in callbacks.
        updateStatus(ThingStatus.UNKNOWN);

        // Allow reconnect attempts
        reconnectJob.enable();

        // Connect the protocol
        connect();
    }

    /**
     * Attempts to connect the {@link BalboaProtocol}
     *
     */
    public void connect() {
        // Reread the configuration
        config = getConfigAs(BalboaConfiguration.class);

        // Connect the protocol
        logger.info("Starting balboa protocol with {} at {}", config.host, config.port);
        protocol.connect(config.host, config.port);
    }

    /**
     * Disposes as {@link BalboaHandler}. The communication protocol with the unit is disconnected.
     */
    @Override
    public void dispose() {
        // Stop any active polling job
        pollingJob.stop();

        // Disallow reconnect attempts and disconnect
        reconnectJob.disable();
        protocol.disconnect();
    }

    /**
     * Handles commands sent to the {@link BalboaHandler}. All channels are handled by inner classes implementing the
     * {@link BalboaChannel} interface. This method will look up the {@link BalboaChannel} in question and pass the
     * command along to it.
     */
    @Override
    public void handleCommand(ChannelUID channelUID, Command command) {
        // Pass the command to the given channel
        if (channels.containsKey(channelUID)) {
            channels.get(channelUID).handleCommand(command);
        } else {
            logger.warn("Command received on unknown channel: {}", channelUID.getAsString());
        }
    }

    /*
     * Block B - Handling events from the communication protocol (status updates and messages).
     *
     * The BalboaProtocol manages the communication with the Balboa Unit. State Changes and Incoming Messages will be
     * passed back to the handler using these callbacks.
     */

    /**
     * Handles state changes on the protocol. The callback receives an enumerated status and a descriptive detail.
     */
    @Override
    public void onStateChange(Status status, String detail) {
        // Stop any active polling job before handling status transitions
        pollingJob.stop();

        // Handle the status transition
        switch (status) {
            case INITIAL:
                // No action is required
                break;
            case CONFIGURATION_PENDING:
                // Report back to the framework that we are waiting for configuration of the unit.
                logger.debug("Balboa Protocol Pending Ponfiguration: {}", detail);
                updateStatus(ThingStatus.ONLINE, ThingStatusDetail.ONLINE.CONFIGURATION_PENDING, detail);
                break;
            case ERROR:
                // Report back to the framework that we have an error. Schedule a reconnect
                logger.debug("Balboa Protocol Error: {}", detail);
                updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.OFFLINE.CONFIGURATION_ERROR, detail);
                reconnectJob.schedule();
                break;
            case OFFLINE:
                // Schedule a reconnect (nothing will happen if reconnects are disabled)
                reconnectJob.schedule();
                // We only update status if we were online (we are in disposal otherwise).
                if (this.getThing().getStatus() == ThingStatus.ONLINE) {
                    updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.OFFLINE.COMMUNICATION_ERROR, detail);
                    logger.info("Balboa Protocol went Offline");
                } else {
                    logger.debug("Balboa Protocol disconnected");
                }
                break;
            case ONLINE:
                updateStatus(ThingStatus.ONLINE);
                logger.info("Balboa Protocol is Online");
                // Start sending poll messages
                pollingJob.start();
                break;
            default:
                break;
        }
    }

    /**
     * Handles messages received by the communication protocol. The callback receives the parsed message. Panel
     * Configuration messages are used to configure the Thing itself (determining what channels it actually has). Other
     * messages are passed to each channel to determine if anything is to be done with it.
     */
    @Override
    public void onMessage(BalboaMessage message) {
        logger.trace("Received a {}", message.getClass().getName());

        // Update the thing configuration (channels) on panel configuration messages
        if (message instanceof BalboaMessage.PanelConfigurationResponseMessage) {
            BalboaMessage.PanelConfigurationResponseMessage config = (PanelConfigurationResponseMessage) message;

            // This only happens once after each connect. Clear the channels we have from before and start over.
            channels.clear();

            // These channels are always there
            channels.addChannel(new TemperatureChannel("current-temperature", "Current Temperature", false));
            channels.addChannel(new TemperatureChannel("target-temperature", "Target Temperature", true));
            channels.addChannel(new TemperatureScale());
            channels.addChannel(new TemperatureRange());
            channels.addChannel(new HeatMode());
            channels.addChannel(new FilterStatus());
            channels.addChannel(new ContactChannel(ItemType.PRIMING, "priming", "Priming", "priming"));
            channels.addChannel(
                    new ContactChannel(ItemType.CIRCULATION, "circulation", "Circulation Pump", "circulation"));
            channels.addChannel(new ContactChannel(ItemType.HEATER, "heater", "Heater", "heater"));

            // Add pumps based on the configuration message. These can be one- or two-speed (Switch or OFF/LOW/HIGH).
            // TODO: Two-speed pumps have not been tested (need a user with such items in the unit)
            for (int i = 0; i < BalboaProtocol.MAX_PUMPS; i++) {
                switch (config.getPump(i)) {
                    // One-speed pump
                    case 0x01:
                        channels.addChannel(new OneSpeedToggle(ItemType.PUMP, i, String.format("pump-%d", i + 1),
                                String.format("Jet Pump %d, one-speed", i + 1), "pump1"));
                        break;
                    // Two-speed pump
                    case 0x02:
                        channels.addChannel(new TwoSpeedToggle(ItemType.PUMP, i, String.format("pump-%d", i + 1),
                                String.format("Jet Pump %d, two-speed", i + 1), "pump2"));
                        break;
                }
            }

            // Add lights based on the configuration message. These can be one- or two-level (Switch or OFF/LOW/HIGH).
            // TODO: Two-level lights have not been tested (need a user with such items in the unit)
            for (int i = 0; i < BalboaProtocol.MAX_LIGHTS; i++) {
                switch (config.getLight(i)) {
                    // One-level light
                    case 0x01:
                        channels.addChannel(new OneSpeedToggle(ItemType.LIGHT, i, String.format("light-%d", i + 1),
                                String.format("Light %d, one-level", i + 1), "light1"));
                        break;
                    // Two-level light
                    case 0x02:
                        channels.addChannel(new TwoSpeedToggle(ItemType.LIGHT, i, String.format("light-%d", i + 1),
                                String.format("Light %d, two-level", i + 1), "light2"));
                        break;
                }
            }

            // Add aux items based on the configuration message. These are always one-speed.
            // TODO: Not tested (need a user with such items in the unit)
            for (int i = 0; i < BalboaProtocol.MAX_AUX; i++) {
                if (config.getAux(i)) {
                    channels.addChannel(new OneSpeedToggle(ItemType.AUX, i, String.format("aux-%d", i + 1),
                            String.format("AUX %d", i + 1), "aux"));
                }
            }

            // Blower. These can be one- or two-speed (Switch or OFF/LOW/HIGH).
            // TODO: Two-speed blowers have not been tested (need a user with such items in the unit).
            switch (config.getBlower()) {
                // One-speed blower
                case 0x01:
                    channels.addChannel(
                            new OneSpeedToggle(ItemType.BLOWER, 0, "blower", "Blower, one-speed", "blower1"));
                    break;
                // Two-speed blower
                case 0x02:
                    channels.addChannel(
                            new TwoSpeedToggle(ItemType.BLOWER, 0, "blower", "Blower, two-speed", "blower2"));
                    break;
            }

            // Mister. These can be one- or two-speed (Switch or OFF/LOW/HIGH).
            // TODO: Two-speed misters have not been tested (need a user with such items in the unit).
            switch (config.getMister()) {
                // One-speed mister
                case 0x01:
                    channels.addChannel(
                            new OneSpeedToggle(ItemType.MISTER, 0, "mister1", "Mister, one-speed", "mister1"));
                    break;
                // Two-speed mister
                case 0x02:
                    channels.addChannel(
                            new TwoSpeedToggle(ItemType.MISTER, 0, "mister2", "Mister, two-speed", "mister2"));
                    break;
            }

            // Build the channels on the thing. Start by wiping all channels existing (e.g. from a previous connect)
            ThingBuilder builder = editThing().withoutChannels(getThing().getChannels());
            // Add the channels determined above.
            for (BalboaChannel channel : channels.values()) {
                builder.withChannel(channel.getChannel());
            }

            // Update the thing with the channels
            updateThing(builder.build());

        } else {
            // Any other message type is passed to the respective channels to determine state updates
            for (BalboaChannel channel : channels.values()) {
                channel.handleUpdate(message);
            }
        }
    }

    /*
     * Block C - Passing updates to/from the framework from/to the balboa unit.
     *
     * This is implemented as a set of classes implementing a common interface (BalboaChannel) since many of the exposed
     * items have things in common. A hierarchy of classes in turn implement this interface as follows:
     * - BaseBalboaChannel - implements things common to all channels (building the channel and handling channel UID's)
     * |- ContactChannel - handles binary read only states received from the protocol
     * |- OneSpeedToggle - handles binary read/write states with the protocol (Switches)
     * |- TwoSpeedToggle - handles (OFF/LOW/HIGH) channels (as Strings with coded values)
     * |- Specific handlers for HeatMode, TemperatureChannel, TemperatureScale, TemperatureRange and FilterStatus
     */

    // We need to track what temperature scale and range we are currently at, in order to form update messages properly.
    private boolean celciusDisplay, temperatureHighRange;

    /**
     * Channels exposed by the Balboa Unit are handled by classes implementing the {@link BalboaChannel} interface.
     *
     * @author CarlÖnnheim
     *
     */
    private interface BalboaChannel {
        /**
         * Returns the {@link ChannelUID} of a {@link BalboaChannel}
         *
         * @return A {@link ChannelUID}
         */
        public ChannelUID getChannelUID();

        /**
         * Returns the {@link Channel} representation of a {@link BalboaChannel}
         *
         * @return A {@link Channel}
         */
        public Channel getChannel();

        /**
         * Handles commands sent to the channel
         *
         * @param command The command sent from the framework
         */
        public void handleCommand(Command command);

        /**
         * Transforms incoming messages from the Balboa Unit to status updates of the {@link Thing}
         *
         * @param message The message received from the communication protocol
         */
        public void handleUpdate(BalboaMessage message);
    }

    /**
     * Base class for channels, implementing common logic
     *
     * @author CarlÖnnheim
     *
     */
    private abstract class BaseBalboaChannel implements BalboaChannel {
        private ChannelUID channelUID;
        private String description;
        private String channelType;
        private String itemType;

        /**
         * Constructs the base channel from a id, description. channel type and item type.
         *
         * @param id
         * @param description
         * @param channelType
         */
        protected BaseBalboaChannel(String id, String description, String channelType, String itemType) {
            this.channelUID = new ChannelUID(thing.getUID(), id);
            this.description = description;
            this.channelType = channelType;
            this.itemType = itemType;
        }

        /**
         * Returns the Channel UID.
         */
        @Override
        public ChannelUID getChannelUID() {
            return channelUID;
        }

        /**
         * Builds the channel.
         */
        @Override
        public Channel getChannel() {
            ChannelBuilder builder = ChannelBuilder.create(channelUID, itemType).withLabel(description)
                    .withDescription(description)
                    .withType(new ChannelTypeUID(BalboaBindingConstants.BINDING_ID, channelType));
            return builder.build();
        }
    }

    /**
     * Handles contact items
     *
     * @author Carl Önnheim
     *
     */
    private class ContactChannel extends BaseBalboaChannel {
        private ItemType balboaItemType;

        /**
         * Instantiate a contact item for the given {@link ItemType}.
         *
         */
        protected ContactChannel(ItemType balboaItemType, String id, String description, String channelType) {
            super(id, description, channelType, "Contact");
            this.balboaItemType = balboaItemType;
        }

        /**
         * Updates will have no effect. The channels are read-on so should never happen in practise
         *
         */
        @Override
        public void handleCommand(Command command) {
            if (command instanceof RefreshType) {
                // Status is sent continuously by the protocol, no action is needed.
            } else {
                logger.warn("Contact channel received update of type {}", command.getClass().getSimpleName());
            }
        }

        /**
         * Updates the channel state from status update messages.
         *
         */
        @Override
        public void handleUpdate(BalboaMessage message) {
            // Only status update messages are of interest
            if (message instanceof BalboaMessage.StatusUpdateMessage) {
                // Get the raw state of the item.
                byte rawState = ((BalboaMessage.StatusUpdateMessage) message).getItem(balboaItemType, 0);
                // Make the update
                updateState(getChannelUID(), rawState == 0 ? OpenClosedType.CLOSED : OpenClosedType.OPEN);
            }
        }
    }

    /**
     * Handles One-Speed toggle items
     *
     * @author CarlÖnnheim
     *
     */
    private class OneSpeedToggle extends BaseBalboaChannel {
        private int index;
        private ItemType balboaItemType;
        private OnOffType state = OnOffType.OFF;

        /**
         * Instantiate a one-speed toggle item.
         *
         * @param index
         */
        protected OneSpeedToggle(ItemType balboaItemType, int index, String id, String description,
                String channelType) {
            super(id, description, channelType, "Switch");

            // Sanity check the index
            if (index < 0 || index >= balboaItemType.count) {
                throw new IllegalArgumentException("Index out of bounds");
            }

            this.balboaItemType = balboaItemType;
            this.index = index;
        }

        /**
         * Set the item to the desired state (ON/OFF)
         */
        @Override
        public void handleCommand(Command command) {
            // Send a toggle if the desired state is not equal to the current state
            if (command instanceof OnOffType) {
                if (command != state) {
                    protocol.sendMessage(new BalboaMessage.ToggleMessage(balboaItemType, index));
                }
            } else if (command instanceof RefreshType) {
                // Status is sent continuously by the protocol, no action is needed.
            } else {
                logger.warn("One-speed channel received update of type {}", command.getClass().getSimpleName());
            }
        }

        /**
         * Updates the channel state from status update messages.
         */
        @Override
        public void handleUpdate(BalboaMessage message) {
            // Only status update messages are of interest
            if (message instanceof BalboaMessage.StatusUpdateMessage) {
                // Get the raw state of the item and determine the OH state.
                byte rawState = ((BalboaMessage.StatusUpdateMessage) message).getItem(balboaItemType, index);
                state = rawState == 0 ? OnOffType.OFF : OnOffType.ON;
                // Make the update
                updateState(getChannelUID(), state);
            }
        }
    }

    /**
     * Handles Two-Speed toggle items
     *
     * @author CarlÖnnheim
     *
     */
    private class TwoSpeedToggle extends BaseBalboaChannel {
        private int index;
        private ItemType balboaItemType;
        private byte rawState;

        /**
         * Instantiate a two-speed toggle item.
         *
         * @param index
         */
        protected TwoSpeedToggle(ItemType balboaItemType, int index, String id, String description,
                String channelType) {
            super(id, description, channelType, "String");

            // Sanity check the index
            if (index < 0 || index >= balboaItemType.count) {
                throw new IllegalArgumentException("Index out of bounds");
            }

            this.balboaItemType = balboaItemType;
            this.index = index;
        }

        /**
         * Set the item to the desired state
         */
        @Override
        public void handleCommand(Command command) {
            if (command instanceof StringType) {
                // Determine how many times to switch (state wraps around 0 -> 1 -> 2 -> 0)
                int count = 0;
                switch (command.toString()) {
                    case "OFF":
                        count = Math.floorMod(0 - rawState, 3);
                        break;
                    case "LOW":
                        count = Math.floorMod(1 - rawState, 3);
                        break;
                    case "HIGH":
                        count = Math.floorMod(2 - rawState, 3);
                        break;
                    default:
                        // Unknown target state, do nothing
                        count = 0;
                        break;
                }
                // Toggle that many times
                for (int i = 0; i < count; i++) {
                    protocol.sendMessage(new BalboaMessage.ToggleMessage(balboaItemType, index));
                }
            } else if (command instanceof RefreshType) {
                // Status is sent continuously by the protocol, no action is needed.
            } else {
                logger.warn("Two-speed channel received update of type {}", command.getClass().getSimpleName());
            }
        }

        /**
         * Updates the channel state from status update messages.
         */
        @Override
        public void handleUpdate(BalboaMessage message) {
            // Only status update messages are of interest
            if (message instanceof BalboaMessage.StatusUpdateMessage) {
                // Get the raw state of the item and determine the OH state.
                rawState = ((BalboaMessage.StatusUpdateMessage) message).getItem(balboaItemType, index);
                StringType state = StringType.EMPTY;
                switch (rawState) {
                    case 0x00:
                        state = StringType.valueOf("OFF");
                        break;
                    case 0x01:
                        state = StringType.valueOf("LOW");
                        break;
                    case 0x02:
                        state = StringType.valueOf("HIGH");
                        break;
                }
                // Make the update
                updateState(getChannelUID(), state);
            }
        }
    }

    /**
     * Handles the Heat Mode
     *
     * @author Carl Önnheim
     *
     */
    private class HeatMode extends BaseBalboaChannel {
        private byte rawState;

        /**
         * Instantiate a heat mode item.
         *
         * @param index
         */
        protected HeatMode() {
            super("heat-mode", "Heat Mode", "heat-mode", "String");
        }

        /**
         * Set the item to the desired state
         */
        @Override
        public void handleCommand(Command command) {
            // Send a toggle if the desired state is not equal to the current state
            if (command instanceof StringType) {
                // Switch based on where the user wants to go
                switch (command.toString()) {
                    case "READY":
                        // We need to toggle if the first bit is set (REST or READY_IN_REST)
                        if ((rawState & 0x01) != 0) {
                            protocol.sendMessage(new BalboaMessage.ToggleMessage(ItemType.HEAT_MODE, 0));
                        }
                        break;
                    case "REST":
                    case "READY_IN_REST":
                        // We need to toggle if the first bit is not set (READY)
                        if ((rawState & 0x01) == 0) {
                            protocol.sendMessage(new BalboaMessage.ToggleMessage(ItemType.HEAT_MODE, 0));
                        }
                        break;
                }
            } else if (command instanceof RefreshType) {
                // Status is sent continuously by the protocol, no action is needed.
            } else {
                logger.warn("Heat Mode channel received update of type {}", command.getClass().getSimpleName());
            }
        }

        /**
         * Updates the channel state from status update messages.
         */
        @Override
        public void handleUpdate(BalboaMessage message) {
            // Only status update messages are of interest
            if (message instanceof BalboaMessage.StatusUpdateMessage) {
                // Get the raw state of the item and determine the OH state.
                rawState = ((BalboaMessage.StatusUpdateMessage) message).getReadyState();
                StringType state = StringType.EMPTY;
                switch (rawState) {
                    case 0x00:
                        state = StringType.valueOf("READY");
                        break;
                    case 0x01:
                        state = StringType.valueOf("REST");
                        break;
                    case 0x03:
                        state = StringType.valueOf("READY_IN_REST");
                        break;
                }
                // Make the update
                updateState(getChannelUID(), state);
            }
        }
    }

    /**
     * Handles the Temperature Scale
     *
     * @author Carl Önnheim
     *
     */
    private class TemperatureScale extends BaseBalboaChannel {

        /**
         * Instantiate a temperature scale item.
         *
         * @param index
         */
        protected TemperatureScale() {
            super("temperature-scale", "Temperature Scale", "temperature-scale", "String");
        }

        /**
         * Set the item to the desired state
         */
        @Override
        public void handleCommand(Command command) {
            // Set the desired temperature scale
            if (command instanceof StringType) {
                switch (command.toString()) {
                    case "C":
                        protocol.sendMessage(new BalboaMessage.SetTemperatureScaleMessage(true));
                        break;
                    case "F":
                        protocol.sendMessage(new BalboaMessage.SetTemperatureScaleMessage(false));
                        break;
                }
            } else if (command instanceof RefreshType) {
                // Status is sent continuously by the protocol, no action is needed.
            } else {
                logger.warn("Temperature Scale channel received update of type {}", command.getClass().getSimpleName());
            }
        }

        /**
         * Updates the channel state from status update messages.
         */
        @Override
        public void handleUpdate(BalboaMessage message) {
            // Only status update messages are of interest
            if (message instanceof BalboaMessage.StatusUpdateMessage) {
                // Remember the state at handler level, since it is needed when setting the target temperature.
                celciusDisplay = ((BalboaMessage.StatusUpdateMessage) message).getCelciusDisplay();
                // Make the update
                updateState(getChannelUID(), celciusDisplay ? StringType.valueOf("C") : StringType.valueOf("F"));
            }
        }
    }

    /**
     * Handles the Temperature Range
     *
     * @author Carl Önnheim
     *
     */
    private class TemperatureRange extends BaseBalboaChannel {

        /**
         * Instantiate a temperature range item.
         *
         * @param index
         */
        protected TemperatureRange() {
            super("temperature-range", "Temperature Range", "temperature-range", "String");
        }

        /**
         * Set the temperature range to the desired state
         */
        @Override
        public void handleCommand(Command command) {
            // Set the desired temperature range
            if (command instanceof StringType) {
                // Toggle if we are not already at the desired state
                switch (command.toString()) {
                    case "LOW":
                        if (temperatureHighRange) {
                            protocol.sendMessage(new BalboaMessage.ToggleMessage(ItemType.TEMPERATURE_RANGE, 0));
                        }
                        break;
                    case "HIGH":
                        if (!temperatureHighRange) {
                            protocol.sendMessage(new BalboaMessage.ToggleMessage(ItemType.TEMPERATURE_RANGE, 0));
                        }
                        break;
                }
            } else if (command instanceof RefreshType) {
                // Status is sent continuously by the protocol, no action is needed.
            } else {
                logger.warn("Temperature Range channel received update of type {}", command.getClass().getSimpleName());
            }
        }

        /**
         * Updates the channel state from status update messages.
         */
        @Override
        public void handleUpdate(BalboaMessage message) {
            // Only status update messages are of interest
            if (message instanceof BalboaMessage.StatusUpdateMessage) {
                // Remember the state at handler level, since it is needed when setting the target temperature.
                temperatureHighRange = ((BalboaMessage.StatusUpdateMessage) message).getItem(ItemType.TEMPERATURE_RANGE,
                        0) != 0x00;
                // Make the update
                updateState(getChannelUID(),
                        temperatureHighRange ? StringType.valueOf("HIGH") : StringType.valueOf("LOW"));
            }
        }
    }

    /**
     * Handles the Temperatures (current temperature and target temperature)
     *
     * @author Carl Önnheim
     *
     */
    private class TemperatureChannel extends BaseBalboaChannel {

        private boolean isTarget;

        /**
         * Instantiate a Temperature item.
         *
         */
        protected TemperatureChannel(String id, String description, boolean isTarget) {
            super(id, description, isTarget ? "target-temperature" : "current-temperature", "Number:Temperature");
            this.isTarget = isTarget;
        }

        /**
         * Sets the target temperature (current temperature is read only)
         */
        @Override
        public void handleCommand(Command command) {
            // Only Quantity Type commands are allowed and only the target temperature is writable
            if (command instanceof QuantityType<?> && isTarget) {
                // Convert to the temperature unit used on the balboa unit
                QuantityType<?> target = (QuantityType<?>) command;
                if (celciusDisplay) {
                    target = target.toUnit(SIUnits.CELSIUS);
                } else {
                    target = target.toUnit(ImperialUnits.FAHRENHEIT);
                }
                // Set the target temperature if the conversion was successful
                if (target != null) {
                    protocol.sendMessage(new BalboaMessage.SetTemperatureMessage(target.doubleValue(), celciusDisplay,
                            temperatureHighRange));
                }
            } else if (command instanceof RefreshType) {
                // Status is sent continuously by the protocol, no action is needed.
            } else {
                logger.warn("{} received update of type {}", this.getChannelUID().getId(),
                        command.getClass().getSimpleName());
            }
        }

        /**
         * Updates the channel state from status update messages.
         */
        @Override
        public void handleUpdate(BalboaMessage message) {
            // Only status update messages are of interest
            if (message instanceof BalboaMessage.StatusUpdateMessage) {
                // Get the raw state
                double rawState = ((BalboaMessage.StatusUpdateMessage) message).getTemperature(isTarget);
                // The unit reports negative numbers if the temperature measurement is unreliable (0xFF which casts to
                // -1.0 double). We discard these altogether.
                if (rawState < 0) {
                    return;
                }
                // Set the proper unit of the value. Get the scale from the message, not the handlers state, since
                // TemperatureScale handler is not necessarily called first.
                QuantityType<Temperature> state;
                if (((BalboaMessage.StatusUpdateMessage) message).getCelciusDisplay()) {
                    state = new QuantityType<Temperature>(rawState, SIUnits.CELSIUS);
                } else {
                    state = new QuantityType<Temperature>(rawState, ImperialUnits.FAHRENHEIT);
                }
                // Make the update
                updateState(getChannelUID(), state);
            }
        }
    }

    /**
     * Handles the Filter Status
     *
     * @author Carl Önnheim
     *
     */
    private class FilterStatus extends BaseBalboaChannel {

        /**
         * Instantiate a Filter status item.
         *
         */
        protected FilterStatus() {
            super("filter", "Filter Status", "filter", "String");
        }

        /**
         * The channel is read only, no action will be taken.
         */
        @Override
        public void handleCommand(Command command) {
            if (command instanceof RefreshType) {
                // Status is sent continuously by the protocol, no action is needed.
            } else {
                logger.warn("Filter Status channel received update of type {}", command.getClass().getSimpleName());
            }
        }

        /**
         * Updates the channel state from status update messages.
         */
        @Override
        public void handleUpdate(BalboaMessage message) {
            // Only status update messages are of interest
            if (message instanceof BalboaMessage.StatusUpdateMessage) {
                // Determine state
                StringType state = StringType.EMPTY;
                switch (((BalboaMessage.StatusUpdateMessage) message).getFilterState()) {
                    case 0x01:
                        state = StringType.valueOf("1");
                        break;
                    case 0x02:
                        state = StringType.valueOf("2");
                        break;
                    case 0x03:
                        state = StringType.valueOf("1+2");
                        break;
                    default:
                        state = StringType.valueOf("OFF");
                        break;
                }
                // Make the update
                updateState(getChannelUID(), state);
            }
        }
    }
}
