use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tauri::ipc::Channel;
use tauri::{async_runtime, command, AppHandle, Runtime};
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};
use uuid::Uuid;

use crate::error::Result;
use crate::models::{AdapterState, BleDevice, ScanFilter, Service, WriteType};
use crate::safety::SafetyStopOutcome;
use crate::{get_handler, OnDisconnectHandler};

/// Notifications buffered per subscription between the BLE callback and the
/// task that forwards them to the front-end. Enough for a burst; the callback
/// never blocks, so anything beyond this is dropped.
const SUBSCRIBE_BUFFER: usize = 128;
/// At most one "dropping notifications" warning per subscription per interval.
const DROP_WARN_INTERVAL: Duration = Duration::from_secs(5);

/// Counts notifications dropped because the forwarding task is behind, and
/// warns at most once per [`DROP_WARN_INTERVAL`].
struct DropWarner {
    start: Instant,
    dropped: AtomicU64,
    /// ms since `start` of the last warning, `u64::MAX` before the first
    last_warn_ms: AtomicU64,
}

impl DropWarner {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            dropped: AtomicU64::new(0),
            last_warn_ms: AtomicU64::new(u64::MAX),
        }
    }

    fn record(&self, characteristic: Uuid) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
        let now = u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX - 1);
        let last = self.last_warn_ms.load(Ordering::Relaxed);
        let due =
            last == u64::MAX || now.saturating_sub(last) >= DROP_WARN_INTERVAL.as_millis() as u64;
        if due
            && self
                .last_warn_ms
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            let dropped = self.dropped.swap(0, Ordering::Relaxed);
            warn!("dropped {dropped} notifications from {characteristic}: the front-end is not keeping up");
        }
    }
}

#[command]
pub(crate) async fn scan<R: Runtime>(
    _app: AppHandle<R>,
    timeout: u64,
    on_devices: Channel<Vec<BleDevice>>,
    allow_ibeacons: bool,
) -> Result<()> {
    tracing::info!("Scanning for BLE devices");
    let handler = get_handler()?;
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    async_runtime::spawn(async move {
        while let Some(devices) = rx.recv().await {
            if let Err(e) = on_devices.send(devices) {
                warn!("Failed to send devices to the front-end: {e}");
                return;
            }
        }
    });
    handler
        .discover(Some(tx), timeout, ScanFilter::None, allow_ibeacons)
        .await?;
    Ok(())
}

#[command]
pub(crate) async fn stop_scan<R: Runtime>(_app: AppHandle<R>) -> Result<()> {
    tracing::info!("Stopping BLE scan");
    let handler = get_handler()?;
    handler.stop_scan().await?;
    Ok(())
}

#[command]
pub(crate) async fn connect<R: Runtime>(
    _app: AppHandle<R>,
    address: String,
    on_disconnect: Channel<()>,
    allow_ibeacons: bool,
) -> Result<()> {
    tracing::info!("Connecting to BLE device: {:?}", address);
    let handler = get_handler()?;
    let disconnct_handler = move || {
        if let Err(e) = on_disconnect.send(()) {
            warn!("Failed to send disconnect event to the front-end: {e}");
        }
    };
    handler
        .connect(
            &address,
            OnDisconnectHandler::from_sync(disconnct_handler),
            allow_ibeacons,
        )
        .await?;
    Ok(())
}

/// Disconnects a BLE device.
/// `address` is optional for backward compatibility: when omitted, the sole
/// connected device is disconnected (an error is returned if zero or more
/// than one device is connected). New multi-device call sites should always
/// pass `address` explicitly.
#[command]
pub(crate) async fn disconnect<R: Runtime>(
    _app: AppHandle<R>,
    address: Option<String>,
) -> Result<()> {
    tracing::info!("Disconnecting from BLE device: {:?}", address);
    let handler = get_handler()?;
    match address {
        Some(address) => handler.disconnect_device(&address).await?,
        None => handler.disconnect().await?,
    }
    Ok(())
}

/// Streams the *aggregate* connection state (true iff at least one device is
/// connected). Kept for backward compatibility with single-device
/// call sites. For a specific device's state use `device_connection_state`.
#[command]
pub(crate) async fn connection_state<R: Runtime>(
    _app: AppHandle<R>,
    update: Channel<bool>,
) -> Result<()> {
    let handler = get_handler()?;
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    handler.set_connection_update_channel(tx).await;
    if let Err(e) = update.send(handler.is_connected()) {
        warn!("Failed to send connection state to the front-end: {e}");
    }
    async_runtime::spawn(async move {
        while let Some(connected) = rx.recv().await {
            if let Err(e) = update.send(connected) {
                warn!("Failed to send connection state to the front-end: {e}");
                return;
            }
        }
        warn!("Connection state channel closed");
    });
    Ok(())
}

/// Streams the connection state of one specific device. Errors immediately
/// if the device is not currently connected (there is nothing to stream
/// updates from yet); callers should invoke this after `connect` resolves.
#[command]
pub(crate) async fn device_connection_state<R: Runtime>(
    _app: AppHandle<R>,
    address: String,
    update: Channel<bool>,
) -> Result<()> {
    let handler = get_handler()?;
    let mut rx = handler.connection_update_receiver(&address).await?;
    if let Err(e) = update.send(*rx.borrow()) {
        warn!("Failed to send device connection state to the front-end: {e}");
    }
    async_runtime::spawn(async move {
        while rx.changed().await.is_ok() {
            let connected = *rx.borrow();
            if let Err(e) = update.send(connected) {
                warn!("Failed to send device connection state to the front-end: {e}");
                return;
            }
        }
    });
    Ok(())
}

#[command]
pub(crate) async fn scanning_state<R: Runtime>(
    _app: AppHandle<R>,
    update: Channel<bool>,
) -> Result<()> {
    let handler = get_handler()?;
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    handler.set_scanning_update_channel(tx).await;
    if let Err(e) = update.send(handler.is_scanning().await) {
        warn!("failed to send scanning state to the front-end: {e}");
    }
    async_runtime::spawn(async move {
        while let Some(scanning) = rx.recv().await {
            if let Err(e) = update.send(scanning) {
                warn!("failed to send scanning state to the front-end: {e}");
                return;
            }
        }
    });
    Ok(())
}

/// Sends data to a characteristic. `address` is optional for backward
/// compatibility: when omitted, the sole connected device is targeted (an
/// error is returned if zero or more than one device is connected).
#[command]
pub(crate) async fn send<R: Runtime>(
    _app: AppHandle<R>,
    characteristic: Uuid,
    service: Option<Uuid>,
    data: Vec<u8>,
    write_type: WriteType,
    address: Option<String>,
) -> Result<()> {
    trace!("Sending data: {data:?}");
    let handler = get_handler()?;
    match address {
        Some(address) => {
            handler
                .send_data_to(&address, characteristic, service, &data, write_type)
                .await?;
        }
        None => {
            handler
                .send_data(characteristic, service, &data, write_type)
                .await?;
        }
    }
    Ok(())
}

#[command]
pub(crate) async fn recv<R: Runtime>(
    _app: AppHandle<R>,
    characteristic: Uuid,
    service: Option<Uuid>,
    address: Option<String>,
) -> Result<Vec<u8>> {
    let handler = get_handler()?;
    let data = match address {
        Some(address) => {
            handler
                .recv_data_from(&address, characteristic, service)
                .await?
        }
        None => handler.recv_data(characteristic, service).await?,
    };
    Ok(data)
}

#[command]
pub(crate) async fn send_string<R: Runtime>(
    app: AppHandle<R>,
    characteristic: Uuid,
    service: Option<Uuid>,
    data: String,
    write_type: WriteType,
    address: Option<String>,
) -> Result<()> {
    let data = data.as_bytes().to_vec();
    send(app, characteristic, service, data, write_type, address).await
}

#[command]
pub(crate) async fn recv_string<R: Runtime>(
    app: AppHandle<R>,
    characteristic: Uuid,
    service: Option<Uuid>,
    address: Option<String>,
) -> Result<String> {
    let data = recv(app, characteristic, service, address).await?;
    Ok(String::from_utf8(data)?)
}

async fn subscribe_channel(
    characteristic: Uuid,
    service: Option<Uuid>,
    address: Option<String>,
) -> Result<mpsc::Receiver<Vec<u8>>> {
    let handler = get_handler()?;
    let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBE_BUFFER);
    let drops = DropWarner::new();
    let callback = move |data: Vec<u8>| {
        trace!("subscribe_channel: {:?}", data);
        match tx.try_send(data) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => drops.record(characteristic),
            // the forwarding task ended (front-end gone); nothing to do until unsubscribe
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    };
    match address {
        Some(address) => {
            handler
                .subscribe_to(&address, characteristic, service, callback)
                .await?;
        }
        None => {
            handler.subscribe(characteristic, service, callback).await?;
        }
    }
    Ok(rx)
}
#[command]
pub(crate) async fn subscribe<R: Runtime>(
    _app: AppHandle<R>,
    characteristic: Uuid,
    service: Option<Uuid>,
    on_data: Channel<Vec<u8>>,
    address: Option<String>,
) -> Result<()> {
    let mut rx = subscribe_channel(characteristic, service, address).await?;
    async_runtime::spawn(async move {
        while let Some(data) = rx.recv().await {
            if let Err(e) = on_data.send(data) {
                // e.g. the page was reloaded; stop forwarding quietly
                debug!("notification receiver for {characteristic} is gone: {e}");
                break;
            }
        }
    });
    Ok(())
}

#[command]
pub(crate) async fn subscribe_string<R: Runtime>(
    _app: AppHandle<R>,
    characteristic: Uuid,
    service: Option<Uuid>,
    on_data: Channel<String>,
    address: Option<String>,
) -> Result<()> {
    let mut rx = subscribe_channel(characteristic, service, address).await?;
    async_runtime::spawn(async move {
        while let Some(data) = rx.recv().await {
            trace!("subscribe_string: {:?}", data);
            let data = String::from_utf8_lossy(&data).into_owned();
            if let Err(e) = on_data.send(data) {
                debug!("notification receiver for {characteristic} is gone: {e}");
                break;
            }
        }
    });
    Ok(())
}

#[command]
pub(crate) async fn unsubscribe<R: Runtime>(
    _app: AppHandle<R>,
    characteristic: Uuid,
    address: Option<String>,
) -> Result<()> {
    let handler = get_handler()?;
    match address {
        Some(address) => handler.unsubscribe_from(&address, characteristic).await?,
        None => handler.unsubscribe(characteristic).await?,
    }
    Ok(())
}

/// Async on purpose. Tauri runs a sync command while it holds its plugin store lock, and on
/// Android this command waits for the user to answer a system permission dialog. The main
/// thread needs the same lock (for example when a page finishes loading), so a sync handler
/// froze the app — "isn't responding" — until the dialog was answered.
#[command]
pub(crate) async fn check_permissions<R: Runtime>(
    _app: AppHandle<R>,
    ask_if_denied: bool,
) -> Result<bool> {
    tokio::task::spawn_blocking(move || crate::check_permissions(ask_if_denied)).await?
}

#[command]
pub(crate) async fn list_services<R: Runtime>(
    _app: tauri::AppHandle<R>,
    address: String,
) -> Result<Vec<Service>> {
    let handler = get_handler()?;
    // Discovery is also used to verify anonymous scan candidates. Nearby
    // non-target devices commonly reject GATT connections; propagate that as
    // a normal command error instead of panicking and terminating the app.
    let services = handler.discover_services(&address).await?;
    Ok(services)
}

/// Lists all currently-connected devices (there may be more than one).
#[command]
pub(crate) async fn connected_devices<R: Runtime>(_app: AppHandle<R>) -> Result<Vec<BleDevice>> {
    let handler = get_handler()?;
    Ok(handler.connected_devices().await)
}

#[command]
pub(crate) async fn get_adapter_state<R: Runtime>(_app: AppHandle<R>) -> Result<AdapterState> {
    let handler = get_handler()?;
    let state = handler.get_adapter_state().await;
    Ok(state)
}

/// Returns the MTU of a connected device. `address` is optional for
/// backward compatibility: when omitted, the sole connected device is
/// targeted (an error is returned if zero or more than one device is
/// connected).
#[command]
pub(crate) async fn mtu<R: Runtime>(_app: AppHandle<R>, address: Option<String>) -> Result<u16> {
    let handler = get_handler()?;
    let mtu = match address {
        Some(address) => handler.mtu_of(&address).await?,
        None => handler.mtu().await?,
    };
    Ok(mtu)
}

/// Registers or replaces the safety stop recipe for a connected device. The
/// plugin never interprets `packet`; it writes it to `characteristic` when a stop
/// is needed. `watchdog_ms` (at least 300) writes the stop once if no `send` to
/// the device happens within that time.
#[command]
pub(crate) async fn set_safety_stop<R: Runtime>(
    _app: AppHandle<R>,
    address: String,
    service: Uuid,
    characteristic: Uuid,
    packet: Vec<u8>,
    watchdog_ms: Option<u64>,
) -> Result<()> {
    let handler = get_handler()?;
    handler
        .set_safety_stop(&address, service, characteristic, packet, watchdog_ms)
        .await
}

#[command]
pub(crate) async fn clear_safety_stop<R: Runtime>(
    _app: AppHandle<R>,
    address: String,
) -> Result<()> {
    let handler = get_handler()?;
    handler.clear_safety_stop(&address);
    Ok(())
}

/// Writes the stop packet preemptively to one device, or to every device with a
/// recipe when `address` is omitted. Returns the outcome per device and errors
/// only if every attempted device failed.
#[command]
pub(crate) async fn safety_stop<R: Runtime>(
    _app: AppHandle<R>,
    address: Option<String>,
) -> Result<Vec<SafetyStopOutcome>> {
    let handler = get_handler()?;
    handler.safety_stop(address.as_deref()).await
}

#[command]
pub(crate) fn set_write_behavior<R: Runtime>(
    _app: AppHandle<R>,
    timeout_in_ms: Option<u32>,
    skip_waiting_on_success: bool,
) -> Result<()> {
    let handler = get_handler()?;
    handler.set_write_behaviour(timeout_in_ms, skip_waiting_on_success);
    Ok(())
}

#[cfg(target_os = "android")]
#[command]
pub(crate) fn set_android_mtu<R: Runtime>(_app: AppHandle<R>, mtu: u16) -> Result<()> {
    crate::handler::Handler::set_android_mtu_request(mtu);
    Ok(())
}

#[cfg(not(target_os = "android"))]
#[command]
pub(crate) fn set_android_mtu<R: Runtime>(_app: AppHandle<R>, _mtu: u16) -> Result<()> {
    Ok(())
}

pub fn commands<R: Runtime>() -> impl Fn(tauri::ipc::Invoke<R>) -> bool {
    tauri::generate_handler![
        scan,
        stop_scan,
        connect,
        disconnect,
        connection_state,
        device_connection_state,
        send,
        send_string,
        recv,
        recv_string,
        subscribe,
        subscribe_string,
        unsubscribe,
        scanning_state,
        check_permissions,
        list_services,
        connected_devices,
        get_adapter_state,
        mtu,
        set_write_behavior,
        set_android_mtu,
        set_safety_stop,
        clear_safety_stop,
        safety_stop
    ]
}
