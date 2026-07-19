use crate::error::Error;
use crate::models::{self, AdapterState, BleDevice, ScanFilter, Service};
use crate::ALLOW_IBEACONS;
use btleplug::api::{Central, Characteristic, Manager as _, Peripheral as _};
use btleplug::api::{CentralEvent, CentralState};
use btleplug::platform::PeripheralId;
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::sleep;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

#[cfg(target_os = "android")]
use crate::android::{Adapter, Manager, Peripheral};
#[cfg(not(target_os = "android"))]
use btleplug::platform::{Adapter, Manager, Peripheral};

struct Listener {
    uuid: Uuid,
    service: Uuid,
    callback: SubscriptionHandler,
}

/// All the state associated with a single, currently-active BLE connection.
///
/// Historically this plugin tracked exactly one of these globally (as a
/// `Mutex<Option<Peripheral>>` plus a handful of loose fields on `Handler`).
/// That meant connecting to a second device silently stole/replaced the
/// first device's slot: writes/reads/subscriptions issued afterwards would
/// silently target the wrong peripheral, and disconnect events for the
/// "orphaned" first device were logged as "unexpected" and dropped instead
/// of running its `on_disconnect` callback.
///
/// Now every connected device gets its own `Connection`, keyed by BLE
/// address in `Handler::connections`, so operations on one device can never
/// interfere with another concurrently-connected device.
struct Connection {
    peripheral: Peripheral,
    characs: Vec<Characteristic>,
    listen_handle: Option<tokio::task::JoinHandle<()>>,
    on_disconnect: OnDisconnectHandler,
    notify_listeners: Arc<Mutex<Vec<Listener>>>,
    /// Serializes GATT operations (read/write/subscribe/etc.) for *this*
    /// device only. Because the lock lives on the `Connection` (not
    /// globally on `Handler` as it used to), operations against different
    /// concurrently-connected devices can proceed fully in parallel, while
    /// operations against the *same* device are still serialized exactly
    /// like before.
    gatt_op_lock: Arc<Mutex<()>>,
    /// Flips to `true`/`false` when the adapter-level `DeviceConnected` /
    /// `DeviceDisconnected` event for this specific device arrives. Used to
    /// synchronize `connect_device`/`disconnect_device` with the actual
    /// hardware event instead of assuming success immediately.
    connected_tx: watch::Sender<bool>,
    connected_rx: watch::Receiver<bool>,
}

impl Connection {
    fn get_charac(&self, uuid: Uuid) -> Result<&Characteristic, Error> {
        trace!("getting characteristic {uuid}");
        let charac = self.characs.iter().find(|c| c.uuid == uuid);
        charac.ok_or(Error::CharacNotAvailable(uuid.to_string()))
    }

    fn get_charac_from_service(&self, uuid: Uuid, service: Uuid) -> Result<&Characteristic, Error> {
        trace!("getting characteristic {uuid} from service {service}");
        let charac = self
            .characs
            .iter()
            .find(|c| c.uuid == uuid && c.service_uuid == service);
        charac.ok_or(Error::CharacNotAvailable(uuid.to_string()))
    }
}

struct HandlerState {
    connection_update_channel: Vec<mpsc::Sender<bool>>,
    scan_update_channel: Vec<mpsc::Sender<bool>>,
    scan_task: Option<tokio::task::JoinHandle<()>>,
}

pub struct Handler {
    devices: Arc<Mutex<HashMap<String, Peripheral>>>,
    adapter: Mutex<Option<Arc<Adapter>>>,
    /// All currently-connected devices, keyed by address. This is the
    /// central piece of multi-device support: previously there was a single
    /// `connected_dev: Mutex<Option<Peripheral>>` slot here instead of a map.
    connections: Arc<Mutex<HashMap<String, Connection>>>,
    /// Aggregate flag: `true` iff *at least one* device is connected.
    /// Kept for backward compatibility with the old single-device API
    /// (`is_connected()`, the `connection_state` IPC channel) which only
    /// ever needs to know "is anything connected". For per-device status use
    /// [`Handler::is_connected_to`] / [`Handler::connection_update_receiver`].
    connected_tx: watch::Sender<bool>,
    connected_rx: watch::Receiver<bool>,
    state: Mutex<HandlerState>,

    write_timeout_in_ms: AtomicU32,
    skip_waiting_for_write_to_complete: AtomicBool,
}

impl Handler {
    pub fn set_write_behaviour(&self, timeout_in_ms: Option<u32>, skip_waiting_on_success: bool) {
        self.write_timeout_in_ms.store(
            timeout_in_ms.unwrap_or(0),
            std::sync::atomic::Ordering::Release,
        );
        self.skip_waiting_for_write_to_complete.store(
            skip_waiting_on_success,
            std::sync::atomic::Ordering::Release,
        );
    }

    #[allow(dead_code)] // TODO: remove once implemented on all platforms
    pub(crate) fn get_write_behaviour(&self) -> (u32, bool) {
        let timeout = self
            .write_timeout_in_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        let skip_waiting_on_success = self
            .skip_waiting_for_write_to_complete
            .load(std::sync::atomic::Ordering::Relaxed);
        (timeout, skip_waiting_on_success)
    }

    /// Sets the MTU that is requested when connecting on Android.
    /// Other plarforms will always negotiate the max by default
    /// The actual MTU can be retrieved using the `mtu`/`mtu_of` method after connecting
    /// 0 means no mtu request will be made
    #[cfg(target_os = "android")]
    pub fn set_android_mtu_request(mtu: u16) {
        crate::android::REQUESTED_MTU.store(mtu, std::sync::atomic::Ordering::Release);
    }
}

async fn get_central() -> Result<Adapter, Error> {
    let manager = Manager::new().await?;
    let adapters = manager.adapters().await?;
    let central = adapters.into_iter().next().ok_or(Error::NoAdapters)?;
    Ok(central)
}

pub enum OnDisconnectHandler {
    None,
    Sync(Box<dyn FnOnce() + Send>),
    Async(Box<dyn FnOnce() -> Box<dyn Future<Output = ()> + Send + Unpin> + Send>),
}
impl OnDisconnectHandler {
    async fn run(self) {
        match self {
            OnDisconnectHandler::None => {}
            OnDisconnectHandler::Sync(f) => f(),
            OnDisconnectHandler::Async(f) => f().await,
        }
    }

    #[must_use]
    pub fn take(&mut self) -> Self {
        std::mem::replace(self, OnDisconnectHandler::None)
    }

    pub fn from_async<F, FUTURE>(func: F) -> Self
    where
        F: FnOnce() -> FUTURE + Send + 'static,
        FUTURE: Future<Output = ()> + Send + 'static,
    {
        OnDisconnectHandler::Async(Box::new(move || Box::new(Box::pin(func()))))
    }

    pub fn from_sync<F: FnOnce() + Send + 'static>(func: F) -> Self {
        OnDisconnectHandler::Sync(Box::new(func))
    }
}

#[allow(clippy::type_complexity)]
pub enum SubscriptionHandler {
    Sync(Box<dyn Fn(Vec<u8>) + Send + Sync>),
    Async(Box<dyn Fn(Vec<u8>) -> Box<dyn Future<Output = ()> + Send + Unpin> + Send + Sync>),
}

impl SubscriptionHandler {
    pub fn from_async<F, FUTURE>(func: F) -> Self
    where
        F: Fn(Vec<u8>) -> FUTURE + Send + Sync + 'static,
        FUTURE: Future<Output = ()> + Send + 'static,
    {
        SubscriptionHandler::Async(Box::new(move |data| Box::new(Box::pin(func(data)))))
    }

    async fn run(&self, data: Vec<u8>) {
        match self {
            SubscriptionHandler::Sync(f) => f(data),
            SubscriptionHandler::Async(f) => f(data).await,
        }
    }
}

impl<F: Fn(Vec<u8>) + Send + Sync + 'static> From<F> for SubscriptionHandler {
    fn from(func: F) -> Self {
        SubscriptionHandler::Sync(Box::new(func))
    }
}

impl Handler {
    pub(crate) async fn new() -> Result<Self, Error> {
        let (connected_tx, connected_rx) = watch::channel(false);
        Ok(Self {
            devices: Arc::new(Mutex::new(HashMap::new())),
            adapter: Mutex::new(None),
            connections: Arc::new(Mutex::new(HashMap::new())),
            connected_rx,
            connected_tx,
            state: Mutex::new(HandlerState {
                connection_update_channel: vec![],
                scan_task: None,
                scan_update_channel: vec![],
            }),
            write_timeout_in_ms: AtomicU32::new(0),
            skip_waiting_for_write_to_complete: AtomicBool::new(false),
        })
    }

    async fn get_or_init_adapter(&self) -> Result<Arc<Adapter>, Error> {
        let mut adapter_guard = self.adapter.lock().await;
        if let Some(adapter) = &*adapter_guard {
            return Ok(adapter.clone());
        }

        let central = get_central().await?;
        let arc_adapter = Arc::new(central);

        if let Ok(handler_static) = crate::get_handler() {
            let stream_res = handler_static
                .get_event_stream_internal(arc_adapter.clone())
                .await;
            tauri::async_runtime::spawn(async move {
                if let Ok(mut stream) = stream_res {
                    while let Some(event) = stream.next().await {
                        let _ = handler_static.handle_event(event).await;
                    }
                }
            });
        }

        *adapter_guard = Some(arc_adapter.clone());
        Ok(arc_adapter)
    }

    async fn get_event_stream_internal(
        &self,
        adapter: Arc<Adapter>,
    ) -> Result<Pin<Box<dyn Stream<Item = CentralEvent> + Send>>, Error> {
        let events = adapter.events().await?;
        Ok(events)
    }

    /// Returns true if *at least one* device is connected.
    /// For multi-device setups prefer [`Handler::is_connected_to`] or
    /// [`Handler::connected_addresses`].
    pub fn is_connected(&self) -> bool {
        *self.connected_rx.borrow()
    }

    /// Returns true if the device with the given address is currently connected.
    pub async fn is_connected_to(&self, address: &str) -> bool {
        self.connections.lock().await.contains_key(address)
    }

    /// Returns the addresses of all currently-connected devices.
    pub async fn connected_addresses(&self) -> Vec<String> {
        self.connections.lock().await.keys().cloned().collect()
    }

    /// Resolves "the" connected device for the backward-compatible,
    /// address-less methods (`disconnect`, `send_data`, `recv_data`,
    /// `subscribe`, `unsubscribe`, `mtu`, `connected_device`).
    ///
    /// These wrappers exist so that single-device call sites (the only kind
    /// that existed before multi-device support was added) keep working
    /// unmodified. They only make sense when at most one device is
    /// connected at a time:
    /// - zero connections => `Error::NoDeviceConnected`
    /// - exactly one connection => that device's address
    /// - more than one connection => `Error::AmbiguousDevice`, forcing the
    ///   caller to switch to the address-aware method (`disconnect_device`,
    ///   `send_data_to`, ...) rather than silently guessing which device was
    ///   meant.
    async fn default_address(&self) -> Result<String, Error> {
        let connections = self.connections.lock().await;
        resolve_default_address(connections.keys())
    }

    /// Returns mtu (Maximum Transfer Unit) of the device at `address`.
    /// # Errors
    /// Returns an error if the device is not connected
    pub async fn mtu_of(&self, address: &str) -> Result<u16, Error> {
        let connections = self.connections.lock().await;
        let conn = connections.get(address).ok_or(Error::NoDeviceConnected)?;
        Ok(conn.peripheral.mtu())
    }

    /// Backward-compatible wrapper around [`Handler::mtu_of`] that operates
    /// on the sole connected device. See [`Handler::default_address`].
    /// # Errors
    /// Returns an error if no device (or more than one device) is connected
    pub async fn mtu(&self) -> Result<u16, Error> {
        let address = self.default_address().await?;
        self.mtu_of(&address).await
    }

    /// Returns true if the adapter is scanning
    pub async fn is_scanning(&self) -> bool {
        if let Some(handle) = &self.state.lock().await.scan_task {
            !handle.is_finished()
        } else {
            false
        }
    }

    /// Takes a sender that will be used to send changes in the scanning status
    pub async fn set_scanning_update_channel(&self, tx: mpsc::Sender<bool>) {
        self.state.lock().await.scan_update_channel.push(tx);
    }

    /// Takes a sender that will be used to send changes in the *aggregate*
    /// connection status (true iff at least one device is connected). For
    /// per-device updates use [`Handler::connection_update_receiver`].
    pub async fn set_connection_update_channel(&self, tx: mpsc::Sender<bool>) {
        self.state.lock().await.connection_update_channel.push(tx);
    }

    /// Returns a `watch::Receiver` that reflects the connection state of one
    /// specific device. The receiver starts at the device's current state and
    /// will update whenever it connects/disconnects.
    /// # Errors
    /// Returns an error if the device is not currently connected (there is
    /// no connection-state stream to observe for a device that was never
    /// connected).
    pub async fn connection_update_receiver(
        &self,
        address: &str,
    ) -> Result<watch::Receiver<bool>, Error> {
        let connections = self.connections.lock().await;
        let conn = connections.get(address).ok_or(Error::NoDeviceConnected)?;
        Ok(conn.connected_rx.clone())
    }

    /// Recomputes the aggregate "is anything connected" flag and, if it
    /// changed, notifies both the `connected_rx` watchers and the legacy
    /// `connection_update_channel` listeners.
    async fn refresh_aggregate_connected(&self) {
        let any = !self.connections.lock().await.is_empty();
        if *self.connected_rx.borrow() != any {
            let _ = self.connected_tx.send(any);
            self.send_connection_update(any).await;
        }
    }

    /// Connects to the given address, in addition to any other devices that
    /// are already connected (this plugin supports multiple concurrent
    /// connections; connecting to a new address never disconnects existing
    /// ones).
    /// If a callback is provided, it will be called when the device is disconnected.
    /// Because connecting sometimes fails especially on android, this method tries up to 3 times
    /// before returning an error
    /// # Errors
    /// Returns an error if no devices are found, if the device is already connected,
    /// if the connection fails, or if the service/characteristics discovery fails
    /// # Example
    /// ```no_run
    /// use tauri::async_runtime;
    /// use tauri_plugin_blec::OnDisconnectHandler;
    /// async_runtime::block_on(async {
    ///    let handler = tauri_plugin_blec::get_handler().unwrap();
    ///    handler.connect("00:00:00:00:00:00", OnDisconnectHandler::from_sync(|| println!("disconnected")), false).await.unwrap();
    /// });
    /// ```
    pub async fn connect(
        &'static self,
        address: &str,
        on_disconnect: OnDisconnectHandler,
        allow_ibeacons: bool,
    ) -> Result<(), Error> {
        if self.connections.lock().await.contains_key(address) {
            return Err(Error::AlreadyConnected);
        }
        if self.devices.lock().await.is_empty() {
            self.discover(None, 1000, ScanFilter::None, allow_ibeacons)
                .await?;
        }
        // cancel any running discovery
        let _ = self.stop_scan().await;
        // connect to the given address
        // try up to 3 times before returning an error
        let (conn_tx, conn_rx) = watch::channel(false);
        let mut connected = Ok(());
        for i in 0..3 {
            if let Err(e) = self
                .connect_device(address, conn_tx.clone(), conn_rx.clone())
                .await
            {
                if i < 2 {
                    warn!("Failed to connect device, retrying in 1s: {e}");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                connected = Err(e);
            } else {
                connected = Ok(());
                break;
            }
        }
        if let Err(e) = connected {
            self.connections.lock().await.remove(address);
            error!("Failed to connect device: {e}");
            return Err(e);
        }
        debug!("connecting services");
        // discover service/characteristics (no connections lock held during GATT op)
        let characs = self.connect_services(address).await?;
        {
            debug!("locking connections");
            let mut connections = self.connections.lock().await;
            if let Some(conn) = connections.get_mut(address) {
                // set callback to run on disconnect
                conn.on_disconnect = on_disconnect;
                conn.characs = characs;
                debug!("Starting notification task for {address}");
                if let Some(handle) = conn.listen_handle.take() {
                    handle.abort();
                }
                // start background task for notifications, scoped to this device only
                conn.listen_handle = Some(tokio::task::spawn(listen_notify(
                    conn.peripheral.clone(),
                    conn.notify_listeners.clone(),
                )));
            } else {
                warn!("connection for {address} vanished before finishing setup");
            }
        }
        self.refresh_aggregate_connected().await;
        info!("connecting to {address} done");
        Ok(())
    }

    async fn connect_services(&self, address: &str) -> Result<Vec<Characteristic>, Error> {
        let (peripheral, gatt_op_lock) = {
            let connections = self.connections.lock().await;
            let conn = connections.get(address).ok_or(Error::NoDeviceConnected)?;
            (conn.peripheral.clone(), conn.gatt_op_lock.clone())
        };
        debug!("starting service discovery for {address}");
        {
            let _gatt_guard = gatt_op_lock.lock().await;
            run_with_timeout(peripheral.discover_services(), "discover services").await?;
        }
        debug!("service discovery done for {address}");
        let mut characs = vec![];
        for s in peripheral.services() {
            for c in &s.characteristics {
                characs.push(c.clone());
            }
        }
        Ok(characs)
    }

    /// Ensures a [`Connection`] entry exists for `address` and drives the
    /// actual adapter-level connect, waiting for the corresponding
    /// `DeviceConnected` event (routed to us by [`Handler::handle_connect`]
    /// via `conn_tx`) before returning.
    async fn connect_device(
        &self,
        address: &str,
        conn_tx: watch::Sender<bool>,
        mut conn_rx: watch::Receiver<bool>,
    ) -> Result<(), Error> {
        trace!("connect_device: initiating connection to {address}");
        debug!("connecting to {address}");
        let peripheral = {
            let devices = self.devices.lock().await;
            devices
                .get(address)
                .ok_or(Error::UnknownPeripheral(address.to_string()))?
                .clone()
        };
        {
            let mut connections = self.connections.lock().await;
            connections.entry(address.to_string()).or_insert_with(|| Connection {
                peripheral: peripheral.clone(),
                characs: vec![],
                listen_handle: None,
                on_disconnect: OnDisconnectHandler::None,
                notify_listeners: Arc::new(Mutex::new(vec![])),
                gatt_op_lock: Arc::new(Mutex::new(())),
                connected_tx: conn_tx.clone(),
                connected_rx: conn_rx.clone(),
            });
        }
        if peripheral.is_connected().await? {
            debug!("Device {address} already connected");
            conn_tx.send(true).expect("failed to send connected update");
            return Ok(());
        }
        debug!("Connecting to device {address}");
        {
            let gatt_op_lock = {
                let connections = self.connections.lock().await;
                connections
                    .get(address)
                    .expect("just inserted above")
                    .gatt_op_lock
                    .clone()
            };
            let _gatt_guard = gatt_op_lock.lock().await;
            run_with_timeout(peripheral.connect(), "Connect").await?;
        }
        // wait for the actual connection to be established
        if !*conn_rx.borrow_and_update() {
            info!("waiting for connection event for {address}");
            conn_rx
                .changed()
                .await
                .expect("failed to wait for connection event");
        }
        if !*conn_rx.borrow() {
            // still not connected
            warn!("{address} still not connected after connection event");
            return Err(Error::ConnectionFailed);
        }
        trace!("connect_device: connection established to {address}");
        info!("device {address} connected");
        Ok(())
    }

    /// Disconnects the device at `address`.
    /// This triggers a disconnect and then waits for the actual disconnect event from the adapter.
    /// Other concurrently-connected devices are left untouched.
    /// # Errors
    /// Returns an error if the device is not connected or if the disconnect fails
    /// # Panics
    /// panics if there is an error with handling the internal disconnect event
    pub async fn disconnect_device(&self, address: &str) -> Result<(), Error> {
        trace!("disconnect: user-initiated disconnect for {address}");
        info!("disconnect triggered by user for {address}");
        let (peripheral, mut conn_rx) = {
            let connections = self.connections.lock().await;
            let conn = connections.get(address).ok_or(Error::NoDeviceConnected)?;
            (conn.peripheral.clone(), conn.connected_rx.clone())
        };
        if let Ok(true) = peripheral.is_connected().await {
            assert!(
                (*conn_rx.borrow_and_update()),
                "connected_rx is false with {address} being connected, this is a bug"
            );
            peripheral.disconnect().await?;
        } else {
            debug!("{address} is not actually connected, cleaning up local state");
            self.handle_disconnect(peripheral.id()).await?;
            return Err(Error::NoDeviceConnected);
        }
        // the change will be triggered by handle_event -> handle_disconnect which runs in another
        // task
        conn_rx
            .changed()
            .await
            .expect("failed to wait for disconnect event");
        if *conn_rx.borrow() {
            // still connected
            return Err(Error::DisconnectFailed);
        }
        Ok(())
    }

    /// Backward-compatible wrapper around [`Handler::disconnect_device`]
    /// that operates on the sole connected device. See
    /// [`Handler::default_address`].
    /// # Errors
    /// Returns an error if no device (or more than one device) is connected, or if the disconnect fails
    pub async fn disconnect(&self) -> Result<(), Error> {
        let address = self.default_address().await?;
        self.disconnect_device(&address).await
    }

    /// Clears internal state for the given device, updates the aggregate
    /// connected flag and calls that device's disconnect callback.
    async fn handle_disconnect(&self, peripheral_id: PeripheralId) -> Result<(), Error> {
        trace!("handle_disconnect: DeviceDisconnected event for {peripheral_id}");
        let address = {
            let connections = self.connections.lock().await;
            connections
                .iter()
                .find(|(_, c)| c.peripheral.id() == peripheral_id)
                .map(|(addr, _)| addr.clone())
        };
        let Some(address) = address else {
            // event not for a device we are tracking, ignore
            warn!("Unexpected disconnect event for device {peripheral_id}, no matching tracked connection");
            return Ok(());
        };
        info!("Handling disconnect for {address} ({peripheral_id})");
        let removed = self.connections.lock().await.remove(&address);
        if let Some(mut conn) = removed {
            *conn.notify_listeners.lock().await = vec![];
            if let Some(handle) = conn.listen_handle.take() {
                handle.abort();
            }
            conn.on_disconnect.take().run().await;
            let _ = conn.connected_tx.send(false);
        }
        self.refresh_aggregate_connected().await;
        Ok(())
    }

    /// Scans for `timeout` milliseconds and periodically sends discovered devices
    /// to the given channel.
    /// A task is spawned to handle the scan and send the devices, so the function
    /// returns immediately.
    ///
    /// A Variant of [`ScanFilter`] can be provided to filter the discovered devices
    /// When `allow_ibeacons` is set to true, android will request fine location permission to
    /// allow finding and connecting to iBeacons.
    ///
    /// # Errors
    /// Returns an error if starting the scan fails
    /// # Panics
    /// Panics if there is an error getting devices from the adapter
    pub async fn discover(
        &'static self,
        tx: Option<mpsc::Sender<Vec<BleDevice>>>,
        timeout: u64,
        filter: ScanFilter,
        allow_ibeacons: bool,
    ) -> Result<(), Error> {
        if let ScanFilter::ManufacturerDataMasked(_, ref data, ref mask) = filter {
            if data.len() != mask.len() {
                return Err(Error::InvalidFilterMask);
            }
        }
        let adapter = self.get_or_init_adapter().await?;
        {
            let mut state = self.state.lock().await;
            // stop any ongoing scan (best-effort — scan may have already
            // been stopped by the polling task)
            if let Some(handle) = state.scan_task.take() {
                handle.abort();
                let _ = adapter.stop_scan().await;
            }
            // start a new scan
            ALLOW_IBEACONS.store(allow_ibeacons, std::sync::atomic::Ordering::Release);
            adapter
                .start_scan(btleplug::api::ScanFilter::default())
                .await?;
        }
        self.send_scan_update(true).await;
        let mut state = self.state.lock().await;
        let mut self_devices = self.devices.clone();
        let adapter = adapter.clone();
        state.scan_task = Some(tokio::task::spawn(async move {
            self_devices.lock().await.clear();
            let loops = std::cmp::max(1, timeout / 200);
            let mut devices;
            for _ in 0..loops {
                sleep(Duration::from_millis(200)).await;
                let mut discovered = adapter
                    .peripherals()
                    .await
                    .expect("failed to get peripherals");
                filter_peripherals(&mut discovered, &filter).await;
                devices = Self::add_devices(&mut self_devices, discovered).await;
                if !devices.is_empty() {
                    if let Some(tx) = &tx {
                        tx.send(devices.clone())
                            .await
                            .expect("failed to send devices");
                    }
                }
            }
            let _ = adapter
                .stop_scan()
                .await
                .map_err(|e| error!("Failed to stop scan: {e}"));
            self.send_scan_update(false).await;
        }));
        Ok(())
    }

    /// Discover provided services and charecteristics
    /// If the device is not connected, a connection is made in order to discover the services and characteristics
    /// After the discovery is done, the device is disconnected
    /// If the devices was already connected, it will stay connected
    /// # Errors
    /// Returns an error if the device is not found, if the connection fails, or if the discovery fails
    /// # Panics
    /// Panics if there is an error with the internal disconnect event
    pub async fn discover_services(&self, address: &str) -> Result<Vec<Service>, Error> {
        let mut already_connected = self.connections.lock().await.contains_key(address);
        let peripheral = if already_connected {
            self.connections
                .lock()
                .await
                .get(address)
                .expect("Connection exists")
                .peripheral
                .clone()
        } else {
            let peripheral = self
                .devices
                .lock()
                .await
                .get(address)
                .ok_or(Error::UnknownPeripheral(address.to_string()))?
                .clone();
            if peripheral.is_connected().await? {
                already_connected = true;
            } else {
                let (conn_tx, conn_rx) = watch::channel(false);
                if let Err(e) = self.connect_device(address, conn_tx, conn_rx).await {
                    self.connections.lock().await.remove(address);
                    error!("Failed to connect for discovery: {e}");
                    return Err(e);
                }
            }
            peripheral
        };
        debug!("discovering services on {address}");
        if peripheral.services().is_empty() {
            let gatt_op_lock = self
                .connections
                .lock()
                .await
                .get(address)
                .map(|c| c.gatt_op_lock.clone());
            if let Some(gatt_op_lock) = gatt_op_lock {
                let _gatt_guard = gatt_op_lock.lock().await;
                run_with_timeout(peripheral.discover_services(), "discover services").await?;
            } else {
                run_with_timeout(peripheral.discover_services(), "discover services").await?;
            }
        }
        let services = peripheral.services().iter().map(Service::from).collect();
        if !already_connected {
            let conn_rx = self
                .connections
                .lock()
                .await
                .get(address)
                .map(|c| c.connected_rx.clone());
            if let Some(mut conn_rx) = conn_rx {
                if *conn_rx.borrow_and_update() {
                    peripheral.disconnect().await?;
                    debug!("waiting for disconnect event");
                    conn_rx
                        .changed()
                        .await
                        .expect("failed to wait for disconnect event");
                }
            }
        }
        Ok(services)
    }

    /// Stops scanning for devices
    /// # Errors
    /// Stops an ongoing scan. The polling task is aborted first, then the
    /// adapter scan is stopped (best-effort — it may have already been
    /// stopped by the polling task finishing).
    pub async fn stop_scan(&self) -> Result<(), Error> {
        if let Some(handle) = self.state.lock().await.scan_task.take() {
            handle.abort();
        }
        let adapter = self.get_or_init_adapter().await?;
        let _ = adapter.stop_scan().await;
        self.send_scan_update(false).await;
        Ok(())
    }

    async fn add_devices(
        self_devices: &mut Arc<Mutex<HashMap<String, Peripheral>>>,
        discovered: Vec<Peripheral>,
    ) -> Vec<BleDevice> {
        let mut devices = vec![];
        for p in discovered {
            match BleDevice::from_peripheral(&p).await {
                Ok(dev) => {
                    self_devices.lock().await.insert(dev.address.clone(), p);
                    devices.push(dev);
                }
                Err(e) => {
                    warn!("Failed to add device: {e}");
                }
            }
        }
        devices.sort();
        devices
    }

    /// Returns the characteristic + a clone of the peripheral/gatt lock for
    /// `address`, looking it up among the currently-connected devices.
    /// The `connections` lock is held only long enough to clone these values
    /// out, never across the actual (potentially slow) GATT operation — this
    /// is what allows operations against different devices to run fully in
    /// parallel.
    async fn resolve_charac(
        &self,
        address: &str,
        c: Uuid,
        service: Option<Uuid>,
    ) -> Result<(Peripheral, Arc<Mutex<()>>, Characteristic), Error> {
        let connections = self.connections.lock().await;
        let conn = connections.get(address).ok_or(Error::NoDeviceConnected)?;
        let charac = if let Some(service) = service {
            conn.get_charac_from_service(c, service)?.clone()
        } else {
            conn.get_charac(c)?.clone()
        };
        Ok((conn.peripheral.clone(), conn.gatt_op_lock.clone(), charac))
    }

    /// Sends data to the given characteristic of the device at `address`.
    /// # Errors
    /// Returns an error if the device is not connected or the characteristic is not available
    /// or if the write operation fails
    pub async fn send_data_to(
        &self,
        address: &str,
        c: Uuid,
        service: Option<Uuid>,
        data: &[u8],
        write_type: models::WriteType,
    ) -> Result<(), Error> {
        let (peripheral, gatt_op_lock, charac) = self.resolve_charac(address, c, service).await?;
        let _gatt_guard = gatt_op_lock.lock().await;
        trace!(
            "sending {} bytes to characteristic {c} on {address}: {:02x?}",
            data.len(),
            data
        );
        peripheral.write(&charac, data, write_type.into()).await?;
        Ok(())
    }

    /// Backward-compatible wrapper around [`Handler::send_data_to`] that
    /// operates on the sole connected device. See [`Handler::default_address`].
    /// # Errors
    /// Returns an error if no device (or more than one device) is connected, the characteristic is not
    /// available, or the write operation fails
    pub async fn send_data(
        &self,
        c: Uuid,
        service: Option<Uuid>,
        data: &[u8],
        write_type: models::WriteType,
    ) -> Result<(), Error> {
        let address = self.default_address().await?;
        self.send_data_to(&address, c, service, data, write_type)
            .await
    }

    /// Receives data from the given characteristic of the device at `address`.
    /// Returns the data as a vector of bytes
    /// # Errors
    /// Returns an error if the device is not connected or the characteristic is not available
    /// or if the read operation fails
    pub async fn recv_data_from(
        &self,
        address: &str,
        c: Uuid,
        service: Option<Uuid>,
    ) -> Result<Vec<u8>, Error> {
        let (peripheral, gatt_op_lock, charac) = self.resolve_charac(address, c, service).await?;
        let _gatt_guard = gatt_op_lock.lock().await;
        let data = run_with_timeout(peripheral.read(&charac), "read").await?;
        trace!(
            "received {} bytes from characteristic {c} on {address}: {:02x?}",
            data.len(),
            data
        );
        Ok(data)
    }

    /// Backward-compatible wrapper around [`Handler::recv_data_from`] that
    /// operates on the sole connected device. See [`Handler::default_address`].
    /// # Errors
    /// Returns an error if no device (or more than one device) is connected, the characteristic is not
    /// available, or the read operation fails
    pub async fn recv_data(&self, c: Uuid, service: Option<Uuid>) -> Result<Vec<u8>, Error> {
        let address = self.default_address().await?;
        self.recv_data_from(&address, c, service).await
    }

    /// Subscribe to notifications from the given characteristic of the device at `address`.
    /// The callback will be called whenever a notification is received
    /// # Errors
    /// Returns an error if the device is not connected or the characteristic is not available
    /// or if the subscribe operation fails
    pub async fn subscribe_to(
        &self,
        address: &str,
        c: Uuid,
        service: Option<Uuid>,
        callback: impl Into<SubscriptionHandler>,
    ) -> Result<(), Error> {
        let (peripheral, gatt_op_lock, charac) = self.resolve_charac(address, c, service).await?;
        let notify_listeners = {
            let connections = self.connections.lock().await;
            connections
                .get(address)
                .ok_or(Error::NoDeviceConnected)?
                .notify_listeners
                .clone()
        };
        let _gatt_guard = gatt_op_lock.lock().await;
        info!("subscribing to characteristic {charac:?} on {address}");
        run_with_timeout(peripheral.subscribe(&charac), "subscribe").await?;
        info!("subscribed successfully to {address}");
        notify_listeners.lock().await.push(Listener {
            uuid: charac.uuid,
            service: charac.service_uuid,
            callback: callback.into(),
        });
        Ok(())
    }

    /// Backward-compatible wrapper around [`Handler::subscribe_to`] that
    /// operates on the sole connected device. See [`Handler::default_address`].
    /// # Errors
    /// Returns an error if no device (or more than one device) is connected, the characteristic is not
    /// available, or the subscribe operation fails
    pub async fn subscribe(
        &self,
        c: Uuid,
        service: Option<Uuid>,
        callback: impl Into<SubscriptionHandler>,
    ) -> Result<(), Error> {
        let address = self.default_address().await?;
        self.subscribe_to(&address, c, service, callback).await
    }

    /// Unsubscribe from notifications for the given characteristic of the device at `address`.
    /// This will also remove the callback from the list of listeners
    /// # Errors
    /// Returns an error if the device is not connected or the characteristic is not available
    /// or if the unsubscribe operation fails
    pub async fn unsubscribe_from(&self, address: &str, c: Uuid) -> Result<(), Error> {
        let (peripheral, gatt_op_lock, charac) = self.resolve_charac(address, c, None).await?;
        let _gatt_guard = gatt_op_lock.lock().await;
        run_with_timeout(peripheral.unsubscribe(&charac), "unsubscribe").await?;
        let notify_listeners = {
            let connections = self.connections.lock().await;
            connections
                .get(address)
                .ok_or(Error::NoDeviceConnected)?
                .notify_listeners
                .clone()
        };
        let mut listeners = notify_listeners.lock().await;
        listeners.retain(|l| l.uuid != charac.uuid);
        Ok(())
    }

    /// Backward-compatible wrapper around [`Handler::unsubscribe_from`] that
    /// operates on the sole connected device. See [`Handler::default_address`].
    /// # Errors
    /// Returns an error if no device (or more than one device) is connected, the characteristic is not
    /// available, or the unsubscribe operation fails
    pub async fn unsubscribe(&self, c: Uuid) -> Result<(), Error> {
        let address = self.default_address().await?;
        self.unsubscribe_from(&address, c).await
    }

    pub(crate) async fn handle_event(&self, event: CentralEvent) -> Result<(), Error> {
        debug!("handling event: {event:?}");
        match event {
            CentralEvent::DeviceDisconnected(peripheral_id) => {
                self.handle_disconnect(peripheral_id).await?;
            }
            CentralEvent::DeviceConnected(peripheral_id) => {
                self.handle_connect(peripheral_id).await;
            }

            _event => {}
        }
        Ok(())
    }

    /// Returns the device connected at `address`.
    /// # Errors
    /// Returns an error if the device is not connected
    pub async fn connected_device_at(&self, address: &str) -> Result<BleDevice, Error> {
        let peripheral = {
            let connections = self.connections.lock().await;
            connections
                .get(address)
                .ok_or(Error::NoDeviceConnected)?
                .peripheral
                .clone()
        };
        let d = BleDevice::from_peripheral(&peripheral).await?;
        Ok(d)
    }

    /// Returns all currently-connected devices.
    pub async fn connected_devices(&self) -> Vec<BleDevice> {
        let peripherals: Vec<Peripheral> = self
            .connections
            .lock()
            .await
            .values()
            .map(|c| c.peripheral.clone())
            .collect();
        let mut devices = vec![];
        for p in peripherals {
            match BleDevice::from_peripheral(&p).await {
                Ok(d) => devices.push(d),
                Err(e) => warn!("Failed to build BleDevice for connected peripheral: {e}"),
            }
        }
        devices
    }

    /// Backward-compatible wrapper around [`Handler::connected_device_at`]
    /// that operates on the sole connected device. See [`Handler::default_address`].
    /// # Errors
    /// Returns an error if no device (or more than one device) is connected
    pub async fn connected_device(&self) -> Result<BleDevice, Error> {
        let address = self.default_address().await?;
        self.connected_device_at(&address).await
    }

    async fn handle_connect(&self, peripheral_id: PeripheralId) {
        let tx = {
            let connections = self.connections.lock().await;
            connections
                .iter()
                .find(|(_, c)| c.peripheral.id() == peripheral_id)
                .map(|(addr, c)| (addr.clone(), c.connected_tx.clone()))
        };
        let Some((address, tx)) = tx else {
            // Not (yet) one of our tracked connections. With multiple
            // concurrent connections there is no single "the" device to
            // fall back to disconnecting here, unlike the old
            // single-connection implementation - we just ignore the event.
            warn!("Received connect event for untracked device {peripheral_id}");
            return;
        };
        trace!("handle_connect: DeviceConnected event for {peripheral_id} ({address})");
        debug!("connection to {address} established");
        tx.send(true).expect("failed to send connected update");
        self.refresh_aggregate_connected().await;
    }

    async fn send_connection_update(&self, state: bool) {
        let tx = &mut self.state.lock().await.connection_update_channel;
        info!("sending connection update to {} listeners", tx.len());
        let mut remove = vec![];
        for (i, t) in tx.iter_mut().enumerate() {
            if let Err(e) = t.send(state).await {
                warn!("Failed to send connection update: {e}");
                remove.push(i);
            }
        }
    }

    async fn send_scan_update(&self, state: bool) {
        let tx = &mut self.state.lock().await.scan_update_channel;
        let mut remove = vec![];
        for (i, t) in tx.iter_mut().enumerate() {
            if let Err(e) = t.send(state).await {
                warn!("Failed to send scan update: {e}");
                remove.push(i);
            }
        }
    }

    pub async fn get_adapter_state(&self) -> AdapterState {
        let adapter = match self.get_or_init_adapter().await {
            Ok(a) => a,
            Err(e) => {
                error!("Failed to init adapter for state check: {e}");
                return AdapterState::Unknown;
            }
        };

        match adapter.adapter_state().await {
            Ok(state) => match state {
                CentralState::Unknown => AdapterState::Unknown,
                CentralState::PoweredOn => AdapterState::On,
                CentralState::PoweredOff => AdapterState::Off,
            },
            Err(e) => {
                error!("Failed to get adapter state: {e}");
                AdapterState::Unknown
            }
        }
    }
}

async fn run_with_timeout<T: Send + Sync + 'static>(
    fut: impl Future<Output = Result<T, btleplug::Error>> + Send,
    cmd: &str,
) -> Result<T, Error> {
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .map_err(|_| Error::Timeout(cmd.to_string()))?
        .map_err(Error::Btleplug)
}

async fn filter_peripherals(discovered: &mut Vec<Peripheral>, filter: &ScanFilter) {
    if matches!(filter, ScanFilter::None) {
        return;
    }
    let mut remove = vec![];
    for p in discovered.iter().enumerate() {
        let Ok(Some(properties)) = p.1.properties().await else {
            // can't filter without properties
            remove.push(p.0);
            continue;
        };
        if properties.rssi.is_none() {
            // ignore not available devices
            remove.push(p.0);
            continue;
        }
        match filter {
            ScanFilter::None => unreachable!("Earyl return for no filter"),
            ScanFilter::Service(uuid) => {
                if !properties.services.iter().any(|s| s == uuid) {
                    remove.push(p.0);
                }
            }
            ScanFilter::AnyService(uuids) => {
                if !properties.services.iter().any(|s| uuids.contains(s)) {
                    remove.push(p.0);
                }
            }
            ScanFilter::AllServices(uuids) => {
                if !uuids.iter().all(|s| properties.services.contains(s)) {
                    remove.push(p.0);
                }
            }
            ScanFilter::ManufacturerData(key, value) => {
                if !properties
                    .manufacturer_data
                    .get(key)
                    .is_some_and(|v| v == value)
                {
                    remove.push(p.0);
                }
            }
            ScanFilter::ManufacturerDataMasked(key, value, maks) => {
                let Some(data) = properties.manufacturer_data.get(key) else {
                    remove.push(p.0);
                    continue;
                };
                if !data
                    .iter()
                    .zip(maks.iter())
                    .zip(value.iter())
                    .all(|((d, m), v)| (d & m) == (*v & m))
                {
                    remove.push(p.0);
                }
            }
        }
    }

    for i in remove.iter().rev() {
        discovered.swap_remove(*i);
    }
}

async fn listen_notify(dev: Peripheral, listeners: Arc<Mutex<Vec<Listener>>>) {
    let mut stream = match dev.notifications().await {
        Ok(stream) => stream,
        Err(e) => {
            error!("failed to get notifications stream: {e}");
            return;
        }
    };
    while let Some(data) = stream.next().await {
        debug!(
            "notification received, listeners: {}",
            listeners.lock().await.len()
        );
        for l in listeners.lock().await.iter() {
            if l.uuid == data.uuid && l.service == data.service_uuid {
                trace!(
                    "notification from {}/{}: {} bytes: {:02x?}",
                    data.service_uuid,
                    data.uuid,
                    data.value.len(),
                    data.value
                );
                // run callback
                trace!("starting callback for {:?}", l.uuid);
                l.callback.run(data.value.clone()).await;
                trace!("callback for {:?} finished", l.uuid);
            }
        }
    }
    info!("Notification stream ended");
}

/// Resolves "the" connected device out of a set of addresses, for the
/// backward-compatible address-less API. Pulled out of
/// [`Handler::default_address`] as a plain, synchronous, hardware-free
/// function so its resolution rules (0 => error, 1 => that address, 2+ =>
/// ambiguous) can be unit tested without spinning up a real `Handler`/BLE
/// adapter.
fn resolve_default_address<'a>(
    addresses: impl Iterator<Item = &'a String>,
) -> Result<String, Error> {
    let mut addresses: Vec<&String> = addresses.collect();
    match addresses.len() {
        0 => Err(Error::NoDeviceConnected),
        1 => Ok(addresses.remove(0).clone()),
        _ => {
            addresses.sort();
            Err(Error::AmbiguousDevice(
                addresses.into_iter().cloned().collect(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_default_address_errors_when_nothing_connected() {
        let addresses: Vec<String> = vec![];
        let result = resolve_default_address(addresses.iter());
        assert!(matches!(result, Err(Error::NoDeviceConnected)));
    }

    #[test]
    fn resolve_default_address_picks_the_sole_connection() {
        let addresses = vec!["AA:BB:CC:DD:EE:FF".to_string()];
        let result = resolve_default_address(addresses.iter());
        assert_eq!(result.unwrap(), "AA:BB:CC:DD:EE:FF");
    }

    #[test]
    fn resolve_default_address_is_ambiguous_with_multiple_connections() {
        // Regression test for the core bug this fork fixes: previously a
        // second concurrent connection silently replaced the first one
        // instead of being tracked independently. Now, address-less calls
        // must fail loudly (rather than silently guessing) once there is
        // more than one concurrently-connected device.
        let addresses = vec![
            "11:11:11:11:11:11".to_string(),
            "00:00:00:00:00:00".to_string(),
            "22:22:22:22:22:22".to_string(),
        ];
        let result = resolve_default_address(addresses.iter());
        match result {
            Err(Error::AmbiguousDevice(reported)) => {
                // sorted, deterministic order
                assert_eq!(
                    reported,
                    vec![
                        "00:00:00:00:00:00".to_string(),
                        "11:11:11:11:11:11".to_string(),
                        "22:22:22:22:22:22".to_string(),
                    ]
                );
            }
            other => panic!("expected AmbiguousDevice, got {other:?}"),
        }
    }
}
