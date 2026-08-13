use tauri::ipc::Channel;
use tauri::{async_runtime, command, AppHandle, Runtime};
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

use crate::error::Result;
use crate::models::{AdapterState, BleDevice, ScanFilter, Service, WriteType};
use crate::{get_handler, OnDisconnectHandler};

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
    info!("Sending data: {data:?}");
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
    Ok(String::from_utf8(data).expect("failed to convert data to string"))
}

async fn subscribe_channel(
    characteristic: Uuid,
    service: Option<Uuid>,
    address: Option<String>,
) -> Result<mpsc::Receiver<Vec<u8>>> {
    let handler = get_handler()?;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let callback = move |data: Vec<u8>| {
        info!("subscribe_channel: {:?}", data);
        tx.try_send(data)
            .expect("failed to send data to the channel");
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
            on_data
                .send(data)
                .expect("failed to send data to the front-end");
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
            info!("subscribe_string: {:?}", data);
            let data = String::from_utf8(data).expect("failed to convert data to string");
            on_data
                .send(data)
                .expect("failed to send data to the front-end");
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

#[command]
pub(crate) fn check_permissions(
    _app: AppHandle<impl Runtime>,
    ask_if_denied: bool,
) -> Result<bool> {
    crate::check_permissions(ask_if_denied)
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
        set_android_mtu
    ]
}
