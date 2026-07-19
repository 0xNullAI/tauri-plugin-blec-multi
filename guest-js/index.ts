import { Channel, invoke } from "@tauri-apps/api/core";

export type BleDevice = {
  address: string;
  name: string;
  rssi: number;
  isConnected: boolean;
  isBonded: boolean;
  services: string[];
  manufacturerData: Record<number, number[]>;
  serviceData: Record<string, number[]>;
  txPowerLevel?: number;
};

export type BleCharacteristic = {
  uuid: string;
  descriptors: string[];
  properties: number;
};

export type BleService = {
  uuid: string;
  characteristics: BleCharacteristic[];
};

export type AdapterState = "Unknown" | "On" | "Off";

/**
 * Get the current state of the BLE adapter (on/off)
 */
export async function getAdapterState(): Promise<AdapterState> {
  let state = await invoke<AdapterState>("plugin:blec|get_adapter_state");
  return state;
}

/**
 * Scan for BLE devices
 * @param handler - A function that will be called with an array of devices found during the scan
 * @param timeout - The scan timeout in milliseconds
 */
export async function startScan(
  handler: (devices: BleDevice[]) => void,
  timeout: Number,
  allowIbeacons: boolean = false
) {
  if (!timeout) {
    timeout = 10000;
  }
  let onDevices = new Channel<BleDevice[]>();
  onDevices.onmessage = handler;
  await invoke<BleDevice[]>("plugin:blec|scan", {
    timeout,
    onDevices,
    allowIbeacons,
  });
}

/**
 * Stop scanning for BLE devices
 */
export async function stopScan() {
  await invoke("plugin:blec|stop_scan");
}

/**
 * Check if necessary permissions are granted
 * @ param askIfDenied - If true, will ask the user for permissions again, if they were denied before
 * @returns true if permissions are granted, false otherwise
 */
export async function checkPermissions(askIfDenied = true): Promise<boolean> {
  return await invoke<boolean>("plugin:blec|check_permissions", { askIfDenied });
}

/**
 * Register a handler to receive updates when the *aggregate* connection
 * state changes (true iff at least one device is connected).
 *
 * Kept for backward compatibility with single-device code. If you connect
 * to more than one device concurrently, use {@link getDeviceConnectionUpdates}
 * instead to track a specific device's connection state.
 */
export async function getConnectionUpdates(
  handler: (connected: boolean) => void
) {
  let connection_chan = new Channel<boolean>();
  connection_chan.onmessage = handler;
  await invoke("plugin:blec|connection_state", { update: connection_chan });
}

/**
 * Register a handler to receive updates when a specific device's connection
 * state changes. Unlike {@link getConnectionUpdates}, this only fires for
 * the given device, so it works correctly when multiple devices are
 * connected concurrently.
 * @param address - The address of the device to track
 * @param handler - A function that will be called with the device's connection state
 */
export async function getDeviceConnectionUpdates(
  address: string,
  handler: (connected: boolean) => void
) {
  let connection_chan = new Channel<boolean>();
  connection_chan.onmessage = handler;
  await invoke("plugin:blec|device_connection_state", {
    address,
    update: connection_chan,
  });
}

/**
 * Register a handler to receive updates when the scanning state changes
 */
export async function getScanningUpdates(handler: (scanning: boolean) => void) {
  let scanning_chan = new Channel<boolean>();
  scanning_chan.onmessage = handler;
  await invoke("plugin:blec|scanning_state", { update: scanning_chan });
}

/**
 * Disconnect from a BLE device.
 *
 * Multiple devices can be connected concurrently. `address` is optional for
 * backward compatibility: when omitted, the sole connected device is
 * disconnected, and the call rejects if zero or more than one device is
 * connected. New code that may have more than one device connected at a
 * time should always pass `address` explicitly.
 * @param address - The address of the device to disconnect. Omit only if exactly one device is connected.
 */
export async function disconnect(address?: string) {
  await invoke("plugin:blec|disconnect", { address });
}

/**
 * Connect to a BLE device. Connecting to a new device does **not** disconnect
 * any other devices that are already connected - multiple devices can be
 * connected at the same time, each tracked independently by address.
 * @param address - The address of the device to connect to
 * @param onDisconnect - A function that will be called when the device disconnects
 */
export async function connect(
  address: string,
  onDisconnect: (() => void) | null,
  allowIbeacons: boolean = false
) {
  let disconnectChannel = new Channel();
  if (onDisconnect) {
    disconnectChannel.onmessage = onDisconnect;
  }
  await invoke("plugin:blec|connect", {
    address: address,
    onDisconnect: disconnectChannel,
    allowIbeacons,
  });
}

/**
 * List all currently-connected devices. Useful once more than one device
 * may be connected at a time.
 */
export async function connectedDevices(): Promise<BleDevice[]> {
  return await invoke<BleDevice[]>("plugin:blec|connected_devices");
}

/**
 * Write a byte array to a BLE characteristic.
 *
 * `address` is optional for backward compatibility: when omitted, the sole
 * connected device is targeted, and the call rejects if zero or more than
 * one device is connected. Pass `address` explicitly when more than one
 * device may be connected at a time.
 * @param characteristic UUID of the characteristic to write to
 * @param data Data to write to the characteristic
 * @param address - The address of the device to write to. Omit only if exactly one device is connected.
 */
export async function send(
  characteristic: string,
  data: number[],
  writeType: "withResponse" | "withoutResponse" = "withResponse",
  service?: string,
  address?: string
) {
  await invoke("plugin:blec|send", {
    characteristic,
    data,
    writeType,
    service,
    address,
  });
}

/**
 * Write a string to a BLE characteristic. See {@link send} for the
 * `address` backward-compatibility note.
 * @param characteristic UUID of the characteristic to write to
 * @param data Data to write to the characteristic
 * @param address - The address of the device to write to. Omit only if exactly one device is connected.
 */
export async function sendString(
  characteristic: string,
  data: string,
  writeType: "withResponse" | "withoutResponse" = "withResponse",
  service?: string,
  address?: string
) {
  await invoke("plugin:blec|send_string", {
    characteristic,
    data,
    writeType,
    service,
    address,
  });
}

/**
 * Read bytes from a BLE characteristic. See {@link send} for the `address`
 * backward-compatibility note.
 * @param characteristic UUID of the characteristic to read from
 * @param address - The address of the device to read from. Omit only if exactly one device is connected.
 */
export async function read(
  characteristic: string,
  service?: string,
  address?: string
): Promise<number[]> {
  let res = await invoke<number[]>("plugin:blec|recv", {
    characteristic,
    service,
    address,
  });
  return res;
}

/**
 * Read a string from a BLE characteristic. See {@link send} for the
 * `address` backward-compatibility note.
 * @param characteristic UUID of the characteristic to read from
 * @param address - The address of the device to read from. Omit only if exactly one device is connected.
 */
export async function readString(
  characteristic: string,
  service?: string,
  address?: string
): Promise<string> {
  let res = await invoke<string>("plugin:blec|recv_string", {
    characteristic,
    service,
    address,
  });
  return res;
}

/**
 * Unsubscribe from a BLE characteristic. See {@link send} for the `address`
 * backward-compatibility note.
 * @param characteristic UUID of the characteristic to unsubscribe from
 * @param address - The address of the device to unsubscribe from. Omit only if exactly one device is connected.
 */
export async function unsubscribe(
  characteristic: string,
  service?: string,
  address?: string
) {
  await invoke("plugin:blec|unsubscribe", {
    characteristic,
    service,
    address,
  });
}

/**
 * Subscribe to a BLE characteristic. See {@link send} for the `address`
 * backward-compatibility note.
 * @param characteristic UUID of the characteristic to subscribe to
 * @param handler Callback function that will be called with the data received for every notification
 * @param address - The address of the device to subscribe to. Omit only if exactly one device is connected.
 */
export async function subscribe(
  characteristic: string,
  service: string | null,
  handler: (data: number[]) => void,
  address?: string
) {
  let onData = new Channel<number[]>();
  onData.onmessage = handler;
  await invoke("plugin:blec|subscribe", {
    characteristic,
    service,
    onData,
    address,
  });
}

/**
 * Subscribe to a BLE characteristic. Converts the received data to a string.
 * See {@link send} for the `address` backward-compatibility note.
 * @param characteristic UUID of the characteristic to subscribe to
 * @param handler Callback function that will be called with the data received for every notification
 * @param address - The address of the device to subscribe to. Omit only if exactly one device is connected.
 */
export async function subscribeString(
  characteristic: string,
  service: string | null,
  handler: (data: string) => void,
  address?: string
) {
  let onData = new Channel<string>();
  onData.onmessage = handler;
  await invoke("plugin:blec|subscribe_string", {
    characteristic,
    service,
    onData,
    address,
  });
}

/**
 * List device services.
 */
export async function listServices(
  address: string
): Promise<BleService[] | string> {
  let res = await invoke<string>("plugin:blec|list_services", {
    address: address,
  });
  return res;
}

/**
 * Get the MTU (Maximum Transfer Unit) of a connected device.
 *
 * `address` is optional for backward compatibility: when omitted, the sole
 * connected device is targeted, and the call rejects if zero or more than
 * one device is connected.
 * @param address - The address of the device to query. Omit only if exactly one device is connected.
 * @returns The MTU value in bytes
 */
export async function getMtu(address?: string): Promise<number> {
  return await invoke<number>("plugin:blec|mtu", { address });
}

/**
 * Configure write behaviour for BLE write operations.
 * @param timeoutInMs - Timeout for write operations in milliseconds. null/undefined means no timeout.
 * @param skipWaitingOnSuccess - If true, do not wait for write completion confirmation on success.
 */
export async function setWriteBehavior(
  timeoutInMs: number | null | undefined,
  skipWaitingOnSuccess: boolean
): Promise<void> {
  await invoke("plugin:blec|set_write_behavior", {
    timeoutInMs,
    skipWaitingOnSuccess,
  });
}

/**
 * Set the MTU that will be requested when connecting on Android.
 * Other platforms negotiate the maximum MTU by default.
 * The actual MTU can be retrieved using `getMtu()` after connecting.
 * @param mtu - The MTU value to request. Use 0 to skip the MTU request.
 */
export async function setAndroidMtu(mtu: number): Promise<void> {
  await invoke("plugin:blec|set_android_mtu", { mtu });
}
