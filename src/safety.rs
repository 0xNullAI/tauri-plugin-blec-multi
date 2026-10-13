//! Native safety stop.
//!
//! Some peripherals are state-retentive: when writes stop they keep doing whatever
//! the last packet told them to. For those the app registers a per-device "stop
//! recipe" (a characteristic and the bytes that make the device stop). The plugin
//! never interprets those bytes; it only knows how to send them fast:
//! preemptively, ahead of any queued write and without waiting for the per-device
//! GATT operation lock.
//!
//! A recipe may carry a watchdog. Every attempted `send` to that device rearms it,
//! and if no send happens within the watchdog period the stop packet is written
//! once. The watchdog then stays quiet until the next send.

use crate::error::Error;
use serde::Serialize;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, warn};
use uuid::Uuid;

/// Shortest watchdog period accepted. The app writes about every 100 ms, so anything
/// shorter would fire on ordinary scheduling jitter.
pub const MIN_WATCHDOG: Duration = Duration::from_millis(300);

/// What to write to make one device stop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopRecipe {
    pub service: Uuid,
    pub characteristic: Uuid,
    pub packet: Vec<u8>,
    pub watchdog: Option<Duration>,
}

impl StopRecipe {
    /// Validates and builds a recipe.
    /// # Errors
    /// Returns [`Error::InvalidSafetyStop`] for an empty packet or a watchdog shorter
    /// than [`MIN_WATCHDOG`].
    pub fn new(
        service: Uuid,
        characteristic: Uuid,
        packet: Vec<u8>,
        watchdog_ms: Option<u64>,
    ) -> Result<Self, Error> {
        if packet.is_empty() {
            return Err(Error::InvalidSafetyStop(
                "the stop packet must not be empty".to_string(),
            ));
        }
        let watchdog = watchdog_ms.map(Duration::from_millis);
        if let Some(watchdog) = watchdog {
            if watchdog < MIN_WATCHDOG {
                return Err(Error::InvalidSafetyStop(format!(
                    "watchdog must be at least {} ms, got {} ms",
                    MIN_WATCHDOG.as_millis(),
                    watchdog.as_millis()
                )));
            }
        }
        Ok(Self {
            service,
            characteristic,
            packet,
            watchdog,
        })
    }
}

/// Result of a safety stop for one device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SafetyStopOutcome {
    pub address: String,
    pub ok: bool,
    pub error: Option<String>,
}

impl SafetyStopOutcome {
    pub(crate) fn from_result(address: String, result: Result<(), Error>) -> Self {
        match result {
            Ok(()) => Self {
                address,
                ok: true,
                error: None,
            },
            Err(e) => Self {
                address,
                ok: false,
                error: Some(e.to_string()),
            },
        }
    }
}

/// Turns per-device outcomes into the command result: an error only if every
/// attempted device failed. No attempted devices is not an error.
pub(crate) fn summarize(outcomes: Vec<SafetyStopOutcome>) -> Result<Vec<SafetyStopOutcome>, Error> {
    if !outcomes.is_empty() && outcomes.iter().all(|o| !o.ok) {
        let details = outcomes
            .iter()
            .map(|o| format!("{}: {}", o.address, o.error.as_deref().unwrap_or("failed")))
            .collect::<Vec<_>>()
            .join("; ");
        return Err(Error::SafetyStopFailed(details));
    }
    Ok(outcomes)
}

pub(crate) type StopFuture = Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>;
/// Writes a recipe's stop packet preemptively. The handler's sink resolves the
/// connection and writes; tests use a fake.
pub(crate) type StopSink = Arc<dyn Fn(String, Arc<StopRecipe>) -> StopFuture + Send + Sync>;

struct Entry {
    recipe: Arc<StopRecipe>,
    /// Rearms the watchdog. Dropping it ends the watchdog task as well.
    kick: Option<watch::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl Drop for Entry {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Registered stop recipes and their watchdogs, keyed by address.
pub(crate) struct SafetyStops {
    // A std mutex on purpose: `kick` runs on every send and never awaits while holding it.
    entries: std::sync::Mutex<HashMap<String, Entry>>,
    sink: StopSink,
}

impl SafetyStops {
    pub(crate) fn new(sink: StopSink) -> Self {
        Self {
            entries: std::sync::Mutex::new(HashMap::new()),
            sink,
        }
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        // a panic elsewhere must not disable the safety stop
        self.entries.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Registers or replaces the recipe for `address`. A watchdog starts armed.
    /// Must be called from within a tokio runtime when the recipe has a watchdog.
    pub(crate) fn set(&self, address: &str, recipe: StopRecipe) {
        let recipe = Arc::new(recipe);
        let (kick, task) = match recipe.watchdog {
            Some(period) => {
                let (tx, rx) = watch::channel(());
                let task = tokio::spawn(watchdog(
                    address.to_string(),
                    recipe.clone(),
                    period,
                    rx,
                    self.sink.clone(),
                ));
                (Some(tx), Some(task))
            }
            None => (None, None),
        };
        // the replaced entry (if any) is dropped here, which aborts its watchdog
        self.entries()
            .insert(address.to_string(), Entry { recipe, kick, task });
    }

    /// Removes the recipe for `address` and cancels its watchdog. Returns whether
    /// one was registered.
    pub(crate) fn clear(&self, address: &str) -> bool {
        self.entries().remove(address).is_some()
    }

    /// Records an attempted send to `address`, rearming its watchdog.
    pub(crate) fn kick(&self, address: &str) {
        if let Some(Entry {
            kick: Some(kick), ..
        }) = self.entries().get(address)
        {
            kick.send_replace(());
        }
    }

    pub(crate) fn recipe(&self, address: &str) -> Option<Arc<StopRecipe>> {
        self.entries().get(address).map(|e| e.recipe.clone())
    }

    pub(crate) fn all(&self) -> Vec<(String, Arc<StopRecipe>)> {
        self.entries()
            .iter()
            .map(|(address, e)| (address.clone(), e.recipe.clone()))
            .collect()
    }
}

async fn watchdog(
    address: String,
    recipe: Arc<StopRecipe>,
    period: Duration,
    mut kick: watch::Receiver<()>,
    sink: StopSink,
) {
    loop {
        match tokio::time::timeout(period, kick.changed()).await {
            // a send happened, start a new period
            Ok(Ok(())) => continue,
            // recipe cleared or replaced
            Ok(Err(_)) => return,
            Err(_) => {
                warn!(
                    "safety watchdog for {address}: no send within {} ms, writing stop packet",
                    period.as_millis()
                );
                match sink(address.clone(), recipe.clone()).await {
                    Ok(()) => debug!("safety watchdog stop for {address} written"),
                    Err(e) => warn!("safety watchdog stop for {address} failed: {e}"),
                }
                // fire once, then wait for the app to send again before rearming
                if kick.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    const ADDR: &str = "AA:BB:CC:DD:EE:FF";

    fn uuid(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn recipe(watchdog_ms: Option<u64>) -> StopRecipe {
        StopRecipe::new(uuid(1), uuid(2), vec![0xB0, 0x00], watchdog_ms).unwrap()
    }

    fn paused_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap()
    }

    type FireLog = Arc<Mutex<Vec<(String, tokio::time::Instant)>>>;

    /// A sink that records every stop it is asked to write.
    fn recording_sink() -> (StopSink, FireLog) {
        let fired = Arc::new(Mutex::new(vec![]));
        let log = fired.clone();
        let sink: StopSink = Arc::new(move |address, _recipe| {
            log.lock()
                .unwrap()
                .push((address, tokio::time::Instant::now()));
            Box::pin(async { Ok(()) })
        });
        (sink, fired)
    }

    async fn sleep_ms(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
        // let woken tasks run before the test inspects state
        tokio::task::yield_now().await;
    }

    #[test]
    fn recipe_rejects_empty_packet() {
        let result = StopRecipe::new(uuid(1), uuid(2), vec![], None);
        assert!(matches!(result, Err(Error::InvalidSafetyStop(_))));
    }

    #[test]
    fn recipe_rejects_short_watchdog() {
        let result = StopRecipe::new(uuid(1), uuid(2), vec![1], Some(299));
        assert!(matches!(result, Err(Error::InvalidSafetyStop(_))));
        let result = StopRecipe::new(uuid(1), uuid(2), vec![1], Some(0));
        assert!(matches!(result, Err(Error::InvalidSafetyStop(_))));
    }

    #[test]
    fn recipe_accepts_minimum_and_no_watchdog() {
        let r = StopRecipe::new(uuid(1), uuid(2), vec![1, 2], Some(300)).unwrap();
        assert_eq!(r.watchdog, Some(Duration::from_millis(300)));
        assert_eq!(r.packet, vec![1, 2]);
        let r = StopRecipe::new(uuid(1), uuid(2), vec![1], None).unwrap();
        assert_eq!(r.watchdog, None);
    }

    #[test]
    fn summarize_errors_only_when_every_device_failed() {
        let ok = SafetyStopOutcome::from_result("A".into(), Ok(()));
        let failed = SafetyStopOutcome::from_result("B".into(), Err(Error::NoDeviceConnected));
        assert!(summarize(vec![]).unwrap().is_empty());
        assert_eq!(
            summarize(vec![ok.clone(), failed.clone()]).unwrap().len(),
            2
        );
        match summarize(vec![failed.clone(), failed]) {
            Err(Error::SafetyStopFailed(msg)) => assert!(msg.contains("B: No device connected")),
            other => panic!("expected SafetyStopFailed, got {other:?}"),
        }
    }

    #[test]
    fn watchdog_fires_once_after_silence_and_rearms_on_send() {
        paused_runtime().block_on(async {
            let (sink, fired) = recording_sink();
            let stops = SafetyStops::new(sink);
            let start = tokio::time::Instant::now();
            stops.set(ADDR, recipe(Some(500)));

            // sends every 100 ms keep it quiet
            for _ in 0..20 {
                sleep_ms(100).await;
                stops.kick(ADDR);
            }
            assert!(fired.lock().unwrap().is_empty());

            // silence: nothing before the period, one stop right after it
            sleep_ms(499).await;
            assert!(fired.lock().unwrap().is_empty());
            sleep_ms(2).await;
            {
                let fired = fired.lock().unwrap();
                assert_eq!(fired.len(), 1);
                assert_eq!(fired[0].0, ADDR);
                assert_eq!(fired[0].1 - start, Duration::from_millis(2500));
            }

            // stays quiet while nothing is sent
            sleep_ms(5000).await;
            assert_eq!(fired.lock().unwrap().len(), 1);

            // the next send rearms it
            stops.kick(ADDR);
            sleep_ms(499).await;
            assert_eq!(fired.lock().unwrap().len(), 1);
            sleep_ms(2).await;
            assert_eq!(fired.lock().unwrap().len(), 2);
        });
    }

    #[test]
    fn watchdog_is_armed_on_registration() {
        paused_runtime().block_on(async {
            let (sink, fired) = recording_sink();
            let stops = SafetyStops::new(sink);
            stops.set(ADDR, recipe(Some(300)));
            sleep_ms(301).await;
            assert_eq!(fired.lock().unwrap().len(), 1);
        });
    }

    #[test]
    fn clearing_cancels_the_watchdog() {
        paused_runtime().block_on(async {
            let (sink, fired) = recording_sink();
            let stops = SafetyStops::new(sink);
            stops.set(ADDR, recipe(Some(500)));
            sleep_ms(200).await;
            assert!(stops.clear(ADDR));
            assert!(!stops.clear(ADDR));
            assert!(stops.recipe(ADDR).is_none());
            sleep_ms(5000).await;
            assert!(fired.lock().unwrap().is_empty());
            // kicking an unknown address is a no-op
            stops.kick(ADDR);
        });
    }

    #[test]
    fn replacing_restarts_the_watchdog() {
        paused_runtime().block_on(async {
            let (sink, fired) = recording_sink();
            let stops = SafetyStops::new(sink);
            stops.set(ADDR, recipe(Some(500)));
            sleep_ms(400).await;
            stops.set(ADDR, recipe(Some(1000)));
            // the old 500 ms timer must not fire
            sleep_ms(500).await;
            assert!(fired.lock().unwrap().is_empty());
            sleep_ms(501).await;
            assert_eq!(fired.lock().unwrap().len(), 1);

            // replacing with no watchdog stops it entirely
            stops.kick(ADDR);
            stops.set(ADDR, recipe(None));
            sleep_ms(5000).await;
            assert_eq!(fired.lock().unwrap().len(), 1);
            assert_eq!(stops.all().len(), 1);
        });
    }

    #[test]
    fn a_slow_stop_does_not_block_sends() {
        paused_runtime().block_on(async {
            // the stop write takes 2 s; kicks during it must not block and must rearm
            let fired = Arc::new(Mutex::new(0usize));
            let count = fired.clone();
            let sink: StopSink = Arc::new(move |_, _| {
                *count.lock().unwrap() += 1;
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    Ok(())
                })
            });
            let stops = SafetyStops::new(sink);
            stops.set(ADDR, recipe(Some(500)));
            sleep_ms(501).await;
            assert_eq!(*fired.lock().unwrap(), 1);
            sleep_ms(100).await;
            stops.kick(ADDR);
            // after the stop finishes (t=2.5 s) the kick rearms a fresh period
            sleep_ms(2398).await;
            assert_eq!(*fired.lock().unwrap(), 1);
            sleep_ms(2).await;
            assert_eq!(*fired.lock().unwrap(), 2);
        });
    }
}
