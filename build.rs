const COMMANDS: &[&str] = &[
    "scan",
    "stop_scan",
    "connect",
    "disconnect",
    "connection_state",
    "send",
    "send_string",
    "recv",
    "recv_string",
    "subscribe",
    "subscribe_string",
    "unsubscribe",
    "scanning_state",
    "check_permissions",
    "list_services",
    "get_adapter_state",
    "set_safety_stop",
    "clear_safety_stop",
    "safety_stop",
];

fn main() {
    tauri_plugin::Builder::new(COMMANDS)
        .android_path("android")
        .build();
}
