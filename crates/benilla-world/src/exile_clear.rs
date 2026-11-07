//! Exile of Azeroth (rehee/benilla fork): the battlefield's clear zone. While a combat is shown,
//! the map's placed M2 doodads (rocks, trees, ruins, fences, carts…) inside an axis-aligned square
//! around the battlefield are not placed when their tile streams in; WMO buildings and the ground
//! clutter (grass) stay. The Exile bridge sets it before the port to the battlefield and clears it
//! before the port home, so the world tiles read under the port's loading screen honour it
//! (user, 2026-11-07: square, doodads only, only during combat).
//!
//! Exile 模式：战斗期间战场周围正方形内不放置小摆件（石头、树、碎墙等），建筑与草保留。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// `[min_x, min_y, max_x, max_y]` in world yards, or none.
static ZONE: RwLock<Option<[f32; 4]>> = RwLock::new(None);
/// Bumped by every change; the terrain stream re-reads its tiles when it moves.
static GENERATION: AtomicU64 = AtomicU64::new(0);

/// Set (a square `half` yards each way from `centre`) or clear the zone.
pub fn set_clear_zone(zone: Option<([f32; 2], f32)>) {
    let square = zone.map(|(c, half)| [c[0] - half, c[1] - half, c[0] + half, c[1] + half]);
    if let Ok(mut z) = ZONE.write() {
        if *z != square {
            *z = square;
            GENERATION.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// The zone's generation (see [`GENERATION`]).
pub fn generation() -> u64 {
    GENERATION.load(Ordering::Relaxed)
}

/// Whether a doodad placed at world `(x, y)` falls inside the zone.
pub fn cleared(x: f32, y: f32) -> bool {
    ZONE.read().ok().and_then(|z| *z).is_some_and(|[x0, y0, x1, y1]| x >= x0 && x <= x1 && y >= y0 && y <= y1)
}
