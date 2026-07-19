# Tauri Plugin blec (multi-device fork)

A BLE-Client plugin based on [btlelug](https://github.com/deviceplug/btleplug).

The main difference to using btleplug directly is that this uses the tauri plugin system for android.
All other platforms use the btleplug implementation.

> **This is a fork of [MnlPhlp/tauri-plugin-blec](https://github.com/MnlPhlp/tauri-plugin-blec)**,
> maintained at [0xNullAI/tauri-plugin-blec-multi](https://github.com/0xNullAI/tauri-plugin-blec-multi).
> All credit for the original plugin design (and the vast majority of the
> code) goes to [Manuel Philipp](https://github.com/MnlPhlp). This fork is
> distributed under the same MIT license as the upstream project (see
> `LICENSE_MIT`/`LICENSE_APACHE`); only the section below and the "Multi-device
> support" changes are new to this fork.
>
> **Why this fork exists:** the upstream plugin is architected around exactly
> one globally-active BLE connection — `connect()`/`disconnect()`/`send()`/
> `read()`/`subscribe()` all implicitly operate on "the currently connected
> device", with no per-device identifier anywhere in the public API. This is
> not an Android or btleplug limitation (Android natively supports several
> concurrent GATT connections, and btleplug's `Peripheral` objects are
> already per-device); it's purely this plugin's own design choice at the
> `Handler` layer. Apps that need to talk to more than one BLE peripheral at
> a time (e.g. several independent BLE-controlled devices) would have a
> second `connect()` call silently steal/replace the first device's
> connection. This fork replaces that single global connection slot with a
> `HashMap<address, Connection>` so multiple devices can be connected and
> operated on independently and concurrently. See "Multi-device support"
> below for details and current status.

## Docs

- [Rust docs (upstream)](https://docs.rs/crate/tauri-plugin-blec/latest) — this fork has not yet been published to docs.rs; read `src/handler.rs` doc comments directly for the moment.
- [JavaScript docs (upstream)](https://mnlphlp.github.io/tauri-plugin-blec/)

## Installation

### Install the rust part of the plugin

```bash
cargo add tauri-plugin-blec
```

Or manually add it to the `src-tauri/Cargo.toml`

```toml
[dependencies]
tauri-plugin-blec = "0.12"
```

### Install the js bindings

use your preferred JavaScript package manager to add `@mnlphlp/plugin-blec`:

```bash
yarn add @mnlphlp/plugin-blec
```

```bash
npm add @mnlphlp/plugin-blec
```

### Register the plugin in Tauri

`src-tauri/src/lib.rs`

```rs
tauri::Builder::default()
    .plugin(tauri_plugin_blec::init())
    .run(tauri::generate_context!())
    .expect("error while running tauri application");
```

```rs
let mut app = tauri::Builder::default();
app = match tauri_plugin_blec::try_init() {
    Ok(plugin) => app.plugin(plugin),
    Err(e) => {
        eprintln!("Failed to initialize blec plugin: {:?}", e);
        app
    }
};
app.run(tauri::generate_context!())
    .expect("error while running tauri application");
```

### Allow calls from Frontend

Add `blec:default` to the permissions in your capabilities file.

[Explanation about capabilities](https://v2.tauri.app/security/capabilities/)

### IOS Setup

Add an entry to the info.plist of your app:

```xml
<key>NSBluetoothAlwaysUsageDescription</key>
<string>The App uses Bluetooth to communicate with BLE devices</string>
```

Add the CoreBluetooth Framework in your xcode procjet:

- open with `tauri ios dev --open`
- click on your project to open settings
- Add Framework under General -> Frameworks,Libraries and Embedded Content

## Usage in Frontend

See [examples/plugin-blec-example](examples/plugin-blec-example) for a full working example that scans for devices, connects and sends/receives data.
In order to use it run [examples/test-server](examples/test-server) on another device and connect to that server.

Short example:

```ts
import { connect, sendString } from '@mnlphlp/plugin-blec'
// get address by scanning for devices and selecting the desired one
let address = ...
// connect and run a callback on disconnect
await connect(address, () => console.log('disconnected'))
// send some text to a characteristic
const CHARACTERISTIC_UUID = '51FF12BB-3ED8-46E5-B4F9-D64E2FEC021B'
await sendString(CHARACTERISTIC_UUID, 'Test', 'withResponse')
```

## Usage in Backend

The plugin can also be used from the rust backend.

The handler returned by `get_handler()` is the same that is used by the frontend commands.
This means if you connect from the frontend you can send data from rust without having to call connect on the backend.

```rs
use uuid::{uuid, Uuid};
use tauri_plugin_blec::models::WriteType;

const CHARACTERISTIC_UUID: Uuid = uuid!("51FF12BB-3ED8-46E5-B4F9-D64E2FEC021B");
const DATA: [u8; 500] = [0; 500];
let handler = tauri_plugin_blec::get_handler().unwrap();
handler
    .send_data(CHARACTERISTIC_UUID, None, &DATA, WriteType::WithResponse)
    .await
    .unwrap();
```

## Multi-device support

This fork can maintain **multiple concurrent BLE connections** at once, each
tracked independently by address. Connecting to a new device never
disconnects any other device that is already connected.

### What changed vs. upstream

- **Rust core (`src/handler.rs`)**: the old `connected_dev: Mutex<Option<Peripheral>>`
  single-connection slot was replaced with `connections: Arc<Mutex<HashMap<String, Connection>>>`,
  where each `Connection` owns its own characteristics cache, notification
  listeners, disconnect callback, and a per-device lock that serializes GATT
  operations *for that device only* — so operations against different
  devices run fully in parallel instead of being globally serialized.
- **New address-aware methods** were added alongside the originals:
  `disconnect_device(address)`, `send_data_to(address, ...)`,
  `recv_data_from(address, ...)`, `subscribe_to(address, ...)`,
  `unsubscribe_from(address, ...)`, `mtu_of(address)`,
  `connected_device_at(address)`, `connected_addresses()`,
  `connected_devices()`, `is_connected_to(address)`.
- **Backward compatibility**: the original address-less methods
  (`disconnect()`, `send_data()`, `recv_data()`, `subscribe()`,
  `unsubscribe()`, `mtu()`, `connected_device()`, `is_connected()`) still
  exist with their original signatures, as thin wrappers that resolve "the"
  connected device automatically. This only works unambiguously when at most
  one device is connected: with zero connections they return
  `Error::NoDeviceConnected` as before; with **more than one** concurrent
  connection they now return `Error::AmbiguousDevice(addresses)` instead of
  silently guessing (or, as upstream did, silently operating on whichever
  device happened to overwrite the single slot last). Existing single-device
  call sites therefore keep working unmodified; call sites that may have
  more than one device connected must switch to the address-aware methods.
- **Tauri commands / JS bindings (`src/commands.rs`, `guest-js/index.ts`)**:
  `disconnect`, `send`, `send_string`, `recv`, `recv_string`, `subscribe`,
  `subscribe_string`, `unsubscribe`, and `mtu` all gained an **optional**
  trailing `address` parameter (same backward-compatible resolution rules as
  above). New commands: `connected_devices` (list all connected devices) and
  `device_connection_state` (per-device connection status stream, exposed in
  JS as `connectedDevices()` and `getDeviceConnectionUpdates()`).
- **Android (`android/`) and the btleplug/Android bridge (`src/android.rs`)**:
  **no changes were needed.** The Kotlin plugin (`BleClientPlugin.kt`)
  already tracked `connected_devices: MutableMap<String, Peripheral>` keyed
  by address, and each Kotlin `Peripheral` already owns its own
  `BluetoothGatt` connection — i.e. the native Android layer already
  supported concurrent connections; the single-connection limitation lived
  entirely in the Rust `Handler`. `src/android.rs` (the btleplug shim that
  talks to the Kotlin plugin) already passed `address` on every plugin
  invocation for the same reason. Two pre-existing, unrelated clippy lints
  in `src/android.rs` were fixed in passing so `cargo clippy --target
  aarch64-linux-android -- -D warnings` is clean.

### What was verified

- `cargo check` / `cargo clippy -- -D warnings` / `cargo build` / `cargo test`
  all pass for the desktop target (`aarch64-apple-darwin`).
- `cargo check` / `cargo clippy -- -D warnings` / `cargo build` all pass for
  `--target aarch64-linux-android` (exercises `src/android.rs`, the code
  path Android actually uses).
- `npx tsc --noEmit` and `npx rollup -c` (the package's real build script)
  both succeed for `guest-js/`.
- New unit tests (`src/handler.rs::tests`, `src/models.rs::tests`) cover the
  default-address resolution rules and the model helpers; there was no prior
  Rust test suite in this repo to extend.
- **A full `cargo tauri android build --debug --target aarch64` against
  `examples/plugin-blec-example` was run end-to-end in this environment**
  (JDK 17, Android SDK, NDK 26.1.10909125, network access to
  dl.google.com/maven.google.com/services.gradle.org were all available)
  and **succeeded**: the modified `tauri-plugin-blec` crate cross-compiled
  cleanly for `aarch64-linux-android`, the modified/unmodified `android/`
  Kotlin sources compiled through Gradle 8.9 + AGP 8.5.1 with zero errors
  (2 pre-existing, unrelated warnings in `Peripheral.kt`), and Gradle
  produced a real `app-universal-debug.apk` (and a matching `.aab`) at
  `examples/plugin-blec-example/src-tauri/gen/android/app/build/outputs/`.
  This proves the whole Rust→JNI→Kotlin plugin compiles and links
  end-to-end with the multi-device changes, on the real Android toolchain,
  not just `cargo check`. Two unrelated fixes were needed to get this far
  and are included in this fork: the example's pinned `tauri`/
  `tauri-plugin-shell` Cargo.toml versions were loosened (they had drifted
  behind the npm-resolved `@tauri-apps/api`/`plugin-shell` versions and
  `cargo tauri` refused to build on the mismatch), and its
  `beforeDevCommand`/`beforeBuildCommand` were switched from `yarn` to
  `npm` (no `yarn`/`corepack` was available in this environment).

### What was **not** verified

- No real BLE hardware test was performed — no BLE adapter/peripherals are
  available in this sandboxed environment, so while the APK builds and
  installs-in-principle, nobody has actually run it and connected to two
  real devices at once to observe the concurrent-connection behavior live.
- No emulator/device was available to actually install and run the built
  APK (no `adb devices` target).
- No iOS testing was performed (this plugin uses btleplug directly on iOS,
  same as upstream; nothing iOS-specific changed here).

### Next steps (for a future session)

1. Install `examples/plugin-blec-example/.../app-universal-debug.apk` on a
   real Android device or emulator and connect to two or more real BLE
   peripherals concurrently to confirm the behavior end-to-end (this is the
   one thing that could not be checked in this sandboxed environment).
2. Wire this fork into DG-Kit's `packages/transport-tauri-blec` (out of
   scope for this fork itself — that package currently assumes the
   single-device upstream API and will need its call sites updated to pass
   `address` explicitly wherever more than one DG device may be connected).
   The relevant DG-Kit files are `packages/transport-tauri-blec/src/plugin-blec.ts`,
   `client.ts`, `characteristic.ts`, and `gatt-shim.ts`.
3. Consider publishing this fork's Rust crate and npm package under new
   names (e.g. `tauri-plugin-blec-multi` / `@0xnullai/plugin-blec-multi`) if
   it is to be consumed the same way upstream is (`cargo add` / `npm add`)
   rather than via a git/path dependency.
