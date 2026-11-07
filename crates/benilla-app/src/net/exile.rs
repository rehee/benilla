//! Exile of Azeroth mode: no WoW server. The Exile Runtime (C#, DQQ) drives the client over a
//! loopback WebSocket, and this module turns its messages into the [`SessionEvent`]s the real IO
//! thread would have produced, so every client system downstream (models, animation, nameplates,
//! FrameXML/Lua addons, glue screens) runs unchanged. See the Exile repository's
//! `docs/architecture.md` §7 and `docs/bridge-protocol.md`.
//!
//! Enabled by `EXILE_RUNTIME=ws://127.0.0.1:7710/`; [`super::io::spawn_net`] then hands its
//! channels here instead of dialling realmd. Login is the env login (`WOW_USER`/`WOW_PASS`), which
//! this module accepts as is.
//!
//! Exile 模式：不连 WoW 服务器，由 Exile Runtime 经本机 WebSocket 驱动客户端（本仓库为 rehee/benilla 分支）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use benilla_protocol::field::{
    FIELD_UNIT_DISPLAYID, FIELD_UNIT_FACTIONTEMPLATE, FIELD_UNIT_HEALTH, FIELD_UNIT_LEVEL,
    FIELD_UNIT_MAXHEALTH,
};
use benilla_protocol::messages::{
    AttackerState, Billing, ChatMessage, ItemInfo, ObjectType, ACCOUNT_DATA_SECTIONS, CHAT_MSG_PARTY,
    LANGUAGE_ADDON,
};
use benilla_protocol::{
    CharEnumItem, Character, EntityKind, LoginStage, MonsterMoveFacing, MoveSpeeds, ObjectFields,
    RealmInfo, RosterCause, SessionEvent,
};
use crossbeam_channel::{select, unbounded, Receiver, Sender};
use serde_json::{json, Value};

use super::io::{NetHandles, PingClock};
use super::{CharRequest, ClientCommand, LoginRequest, ParkRequest, ParkSender, RealmRequest};

/// `OBJECT_FIELD_TYPE` (2), `UNIT_FIELD_BYTES_0` (36: race, class, gender, power type) and
/// `UNIT_FIELD_NATIVEDISPLAYID` (132), 1.12.1 indices.
const FIELD_OBJECT_TYPE: u16 = 2;
/// `OBJECT_FIELD_ENTRY` (3): a unit's creature template, which its name query asks by.
const FIELD_OBJECT_ENTRY: u16 = 3;
/// `OBJECT_FIELD_SCALE_X` (4), the complete render scale, a float.
const FIELD_OBJECT_SCALE_X: u16 = 4;
const FIELD_UNIT_BYTES_0: u16 = 36;
const FIELD_UNIT_NATIVEDISPLAYID: u16 = 132;
/// `PLAYER_BYTES` (193: skin, face, hair style, hair colour) and `PLAYER_BYTES_2` (194: facial
/// hair), and `PLAYER_VISIBLE_ITEM_1_0` (260), each slot's item entry, 12 fields a slot.
const FIELD_PLAYER_BYTES: u16 = 193;
const FIELD_PLAYER_BYTES_2: u16 = 194;
const FIELD_PLAYER_VISIBLE_ITEM_1_0: u16 = 260;
const VISIBLE_ITEM_STRIDE: u16 = 12;
/// A dressed unit's item templates: entry = this + the item's `ItemDisplayInfo` id.
const DRESS_ITEM_BASE: u32 = 800_000;
/// Human faction template (friendly to the player) and a hostile one for enemies.
const FACTION_PLAYER: u32 = 1;
const FACTION_HOSTILE: u32 = 14;

/// Free mode's spot: Northshire, the client's own boot anchor, map 0 (Eastern Kingdoms).
const MAP_ID: u32 = 0;
/// Map 0's directory name under `World\\Maps`.
const MAP_NAME: &str = "Azeroth";
const ORIGIN: [f32; 3] = [-8949.95, -132.49, 83.6];
/// The battlefield (for now one open field in Northshire, architecture §5.2): DQQ's 10×10 field is
/// centred here, one DQQ unit [`YARDS_PER_UNIT`] yards.
const BATTLE_CENTER: [f32; 3] = [-8934.95, -117.49, 83.6];
const DQQ_FIELD_HALF: f64 = 5.0;
const YARDS_PER_UNIT: f32 = 2.0;
/// `.lineup`: models per row, and yards between them at size 1.
const LINEUP_ROW: usize = 6;
const LINEUP_GAP: f32 = 3.5;
/// The lineup's creature entries (names) start here, clear of any real template.
const LINEUP_ENTRY: u32 = 900_000;


/// Units and players whose model has not built yet ([`count_unembodied`]): a combat's start
/// waits for it to reach zero before the runtime runs on ([`Bridge::ready_check`]).
static UNEMBODIED: AtomicU32 = AtomicU32::new(0);

/// Each frame, the units and players still without a model.
pub(super) fn count_unembodied(
    units: bevy::prelude::Query<
        &super::NetEntity,
        bevy::prelude::Without<benilla_assets::ModelAnimations>,
    >,
) {
    let n = units
        .iter()
        .filter(|n| matches!(n.kind, EntityKind::Unit | EntityKind::Player))
        .count();
    UNEMBODIED.store(n as u32, Ordering::Relaxed);
}

/// Is Exile mode on? `EXILE_RUNTIME` names the runtime's WebSocket.
pub(super) fn runtime_url() -> Option<String> {
    std::env::var("EXILE_RUNTIME").ok().filter(|v| !v.trim().is_empty())
}

/// [`super::io::spawn_net`]'s Exile twin: the same handles, served by the bridge thread.
pub(super) fn spawn(url: String) -> NetHandles {
    let (events_tx, events_rx) = unbounded();
    let (cmd_tx, cmd_rx) = unbounded();
    let (park_tx, park_rx) = unbounded::<ParkRequest>();
    let (login_tx, login_rx) = unbounded::<LoginRequest>();
    let (ws_in_tx, ws_in_rx) = unbounded::<Value>();
    let (ws_out_tx, ws_out_rx) = unbounded::<String>();
    thread::Builder::new()
        .name("exile-ws".into())
        .spawn(move || ws_loop(&url, &ws_in_tx, &ws_out_rx))
        .expect("spawn exile-ws thread");
    thread::Builder::new()
        .name("exile-bridge".into())
        .spawn(move || {
            let mut bridge = Bridge::new(events_tx, ws_out_tx);
            bridge.run(&login_rx, &park_rx, &cmd_rx, &ws_in_rx);
        })
        .expect("spawn exile-bridge thread");
    NetHandles {
        events: events_rx,
        commands: cmd_tx,
        pick: ParkSender::park(park_tx.clone()),
        realm: ParkSender::park(park_tx),
        login: login_tx,
        login_abandon: Arc::new(AtomicU64::new(0)),
        ping: Arc::new(Mutex::new(PingClock::default())),
    }
}

/// The WebSocket thread: connects (retrying while the runtime starts), forwards each text message
/// as JSON, and writes what the bridge queues. A short read timeout lets one thread do both.
fn ws_loop(url: &str, inbound: &Sender<Value>, outbound: &Receiver<String>) {
    use tungstenite::{stream::MaybeTlsStream, Message};
    let mut ws = None;
    for attempt in 0..120 {
        match tungstenite::connect(url) {
            Ok((socket, _)) => {
                ws = Some(socket);
                break;
            }
            Err(e) => {
                if attempt % 10 == 0 {
                    bevy::log::warn!("exile: runtime {url} not reachable yet ({e})");
                }
                thread::sleep(Duration::from_millis(500));
            }
        }
    }
    let Some(mut ws) = ws else {
        bevy::log::error!("exile: gave up connecting to the runtime at {url}");
        return;
    };
    if let MaybeTlsStream::Plain(s) = ws.get_mut() {
        let _ = s.set_nodelay(true);
        let _ = s.set_read_timeout(Some(Duration::from_millis(2)));
    }
    bevy::log::info!("exile: connected to the runtime at {url}");
    loop {
        match ws.read() {
            Ok(Message::Text(text)) => match serde_json::from_str::<Value>(text.as_str()) {
                Ok(v) => {
                    if inbound.send(v).is_err() {
                        return;
                    }
                }
                Err(e) => bevy::log::warn!("exile: bad message from the runtime: {e}"),
            },
            Ok(Message::Close(_)) => {
                bevy::log::warn!("exile: the runtime closed the link");
                return;
            }
            Ok(_) => {}
            // The read timeout ending a wait: on Windows it can also surface as
            // ERROR_IO_PENDING (997) or WSAEWOULDBLOCK (10035), which are no failure either.
            Err(tungstenite::Error::Io(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) || matches!(e.raw_os_error(), Some(997 | 10035 | 10060)) => {}
            Err(e) => {
                bevy::log::error!("exile: runtime link failed: {e}");
                return;
            }
        }
        while let Ok(text) = outbound.try_recv() {
            if let Err(e) = ws.send(Message::text(text)) {
                bevy::log::error!("exile: send to the runtime failed: {e}");
                return;
            }
        }
    }
}

/// One DQQ actor shown in the world, with what was last sent for it.
struct Shown {
    guid: u64,
    pos: [f32; 3],
    life: u32,
    /// The facing last shown, WoW radians.
    facing: f32,
    /// The frame the walk segment under way ends on; none runs once [`Bridge::frames`] reaches it.
    walk_until: u64,
    /// Where DQQ had the actor last frame, and its `moving` flag then (the flag census).
    last_dqq: [f32; 3],
    /// A DQQ enemy (else the hero's side).
    enemy: bool,
    /// The frame the walk under way started on (`None`: standing).
    walk_start: Option<u64>,
}

/// A walk that starts plays at least this many frames (user rule, 2026-11-06): each frame's step
/// is sent as it comes (no frame is held back, 2026-11-07), its spline stretched to reach this
/// many frames past the walk's start; a unit still moving goes straight on, the run cycle unbroken.
const SEGMENT_FRAMES: u64 = 5;
/// A step's spline lasts this many frames, so the next step takes over before it ends and the
/// run never gaps to the stance: DQQ's steps come every frame to every third (measured
/// 2026-11-07: at 1.5 frames the run broke into the stance for 16~50 ms between steps). The
/// cost: a walk's end shows up to this long after DQQ stopped.
const STEP_FRAMES: f32 = 4.0;
/// The walk speeds units are created with. The client picks Run over Walk above twice the walk
/// speed, and DQQ's units cover about a yard a second, so in combat the walk speed is set so low
/// that every walk shows the run (user, 2026-11-06); free mode keeps the stock 2.5.
const COMBAT_WALK_SPEED: f32 = 0.05;
const FREE_WALK_SPEED: f32 = 2.5;
/// One DQQ tick, milliseconds (30 ticks/s).
const TICK_MS: f32 = 1000.0 / 30.0;

/// The terrain under the battlefield, read from the install's ADT tiles once, so every actor
/// stands on the ground: a creature's height is the server's (here, ours) to give, not the
/// client's to settle.
struct Ground {
    tiles: Vec<benilla_formats::TileMesh>,
}

impl Ground {
    fn load(around: [f32; 3]) -> Self {
        let tiles = benilla_formats::wow_data()
            .and_then(|data| benilla_formats::open_chain(&data).ok())
            .and_then(|mut chain| {
                benilla_formats::load_tiles_around(&mut chain, MAP_NAME, around[0], around[1], 1).ok()
            })
            .map(|v| v.into_iter().map(|(_, mesh)| mesh).collect::<Vec<_>>())
            .unwrap_or_default();
        let ground = Self { tiles };
        bevy::log::info!(
            "exile: ground {} tiles, battlefield centre z {:.2} (configured {:.2})",
            ground.tiles.len(),
            ground.z(around[0], around[1], around[2]),
            around[2]
        );
        ground
    }

    /// The terrain height at `(x, y)`; `fallback` off the loaded tiles or in a hole.
    fn z(&self, x: f32, y: f32, fallback: f32) -> f32 {
        self.tiles
            .iter()
            .find_map(|t| benilla_formats::terrain_height_at(&t.chunks, [x, y, fallback]))
            .unwrap_or(fallback)
    }

    /// `p` put on the ground.
    fn on_ground(&self, p: [f32; 3]) -> [f32; 3] {
        [p[0], p[1], self.z(p[0], p[1], p[2])]
    }
}

/// How long after entering the world the auto combat starts: the entry load must have drained,
/// or the frames land on a loading screen.
const AUTOCOMBAT_DELAY: Duration = Duration::from_secs(10);

/// Free mode, or where the trip into and out of a combat stands (D27–D29).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Free mode: the player plays the character with the stock controls.
    Free,
    /// Control is taken and the battlefield's loading screen (with its bar) is up; the replay is
    /// asked for when the client acks the port, i.e. when the load is done (D28).
    Porting,
    /// The replay plays: the player's own body is the combat's creator.
    Combat,
    /// The way home: control returns when the client acks that port.
    Returning,
}
/// The combat's start hold: at least, the models' fade-in after the last one builds, at most.
const READY_MIN: Duration = Duration::from_millis(2500);
const READY_SETTLE: Duration = Duration::from_millis(800);
const READY_MAX: Duration = Duration::from_secs(10);
/// How long a finished combat's units stay before they are removed.
const CLEAR_DELAY: Duration = Duration::from_secs(3);

/// Model keys from the runtime (`docs/bridge-protocol.md` ActorState `model`) resolved against the
/// install's `CreatureDisplayInfo`: a number is a display id; `@HumanFemale` the first human female
/// character-model NPC; any other key the first textured creature whose model path contains it.
struct ModelBook {
    catalog: Option<benilla_formats::CreatureCatalog>,
    /// The install's archives, for the models' bounds ([`Self::footprint`]).
    chain: Option<benilla_formats::Chain>,
    resolved: HashMap<String, u32>,
    /// Each display's footprint at scale 1, yards (`None`: unreadable).
    footprints: HashMap<u32, Option<f32>>,
}

impl ModelBook {
    fn load() -> Self {
        let mut chain = benilla_formats::wow_data()
            .and_then(|data| benilla_formats::open_chain(&data).ok());
        let catalog = chain
            .as_mut()
            .and_then(|chain| benilla_formats::load_creature_catalog(chain).ok());
        bevy::log::info!(
            "exile: creature catalog {}",
            catalog.as_ref().map_or(0, benilla_formats::CreatureCatalog::len)
        );
        Self { catalog, chain, resolved: HashMap::new(), footprints: HashMap::new() }
    }

    /// A display's footprint at `OBJECT_FIELD_SCALE_X` 1: the larger horizontal extent of its
    /// model's bounding box, yards.
    fn footprint(&mut self, display: u32) -> Option<f32> {
        if let Some(fp) = self.footprints.get(&display) {
            return *fp;
        }
        let path = self.catalog.as_ref().and_then(|c| c.model(display)).map(|m| m.model_path);
        let fp = path.and_then(|path| {
            let b = benilla_formats::load_m2_bounds(self.chain.as_mut()?, &path).ok()?;
            let w = (b.bbox_max[0] - b.bbox_min[0]).abs().max((b.bbox_max[1] - b.bbox_min[1]).abs());
            (w > 0.0).then_some(w)
        });
        let id = display;
        bevy::log::info!("exile: display {id} footprint {fp:?} yd at scale 1");
        self.footprints.insert(display, fp);
        fp
    }

    /// A creature's render scale (`OBJECT_FIELD_SCALE_X`) for a DQQ actor of `size` (its radius
    /// over a human's, user 2026-11-06): the display's own scale (`modelScale × creatureModelScale`,
    /// which a server folds into the field) times the size; then, unless `oversize` (a specially
    /// marked monster), cut so the model covers at most `size × YARDS_PER_UNIT` yards each way.
    fn unit_scale(&mut self, display: u32, size: f32, oversize: bool) -> f32 {
        let natural = self.catalog.as_ref().and_then(|c| c.model_scale(display)).unwrap_or(1.0);
        let mut scale = natural * size;
        if !oversize {
            if let Some(fp) = self.footprint(display) {
                let (covered, cap) = (fp * scale, size * YARDS_PER_UNIT);
                if covered > cap {
                    scale *= cap / covered;
                }
            }
        }
        scale
    }

    fn resolve(&mut self, key: &str) -> u32 {
        if let Ok(id) = key.parse::<u32>() {
            return id;
        }
        if let Some(&id) = self.resolved.get(key) {
            return id;
        }
        let id = self.search(key).unwrap_or(FALLBACK_DISPLAY);
        bevy::log::info!("exile: model `{key}` → display {id}");
        self.resolved.insert(key.to_owned(), id);
        id
    }

    fn search(&self, key: &str) -> Option<u32> {
        let catalog = self.catalog.as_ref()?;
        let mut ids: Vec<u32> = catalog.display_models().map(|(d, _)| d).collect();
        ids.sort_unstable();
        if key == "@HumanFemale" {
            return ids.into_iter().find(|&d| catalog.display_race_sex(d) == Some((1, 1)));
        }
        let needle = key.to_ascii_lowercase();
        ids.into_iter().find(|&d| {
            catalog.model(d).is_some_and(|m| {
                m.npc_appearance.is_none()
                    && m.textures[0].is_some()
                    && m.model_path.to_ascii_lowercase().contains(&needle)
            })
        })
    }
}

/// The Exile UI addon's message prefix (`addons/ExileUI`).
const ADDON_PREFIX: &str = "EXILE";
/// The most recordings listed to the addon.
const ADDON_REPLAYS: usize = 10;

/// A display every install has: the kobold (`Creature\\Kobold`), when a key finds nothing.
const FALLBACK_DISPLAY: u32 = 30;

/// The bridge's state: the glue answers and the world it has built.
struct Bridge {
    events: Sender<SessionEvent>,
    out: Sender<String>,
    seq: u64,
    realm: RealmInfo,
    epoch: u64,
    generation: u64,
    /// The last roster, and each entry's DQQ character id.
    roster: Vec<Character>,
    dqq_ids: HashMap<u64, String>,
    self_guid: Option<u64>,
    /// The roster entry played, to create the player's own unit again after each worldport.
    me: Option<Character>,
    /// DQQ actor id → its world unit.
    shown: HashMap<String, Shown>,
    next_creature: u64,
    /// The `combatId` being shown.
    combat: Option<String>,
    models: ModelBook,
    phase: Phase,
    /// The combat creator's DQQ actor id, whom the player's own body plays (D28).
    focus: Option<String>,
    /// When the auto combat is due, and when a finished combat's units go.
    combat_due: Option<Instant>,
    clear_due: Option<Instant>,
    spline_id: u32,
    ground: Ground,
    /// The combat's start hold: when its first frame was shown (the runtime waits on
    /// `combat.ready`), and since when every model has been built.
    ready_first: Option<Instant>,
    ready_quiet: Option<Instant>,
    /// `combat.ready` sent, or no hold under way.
    ready_sent: bool,
    /// Each combat unit's attack target, for the combat stance ([`Self::keep_engaged`]).
    engaged: HashMap<u64, u64>,
    /// The recording the next replay trip plays (`None`: the character's latest).
    replay_id: Option<String>,
    /// Or the recording file it plays (`replays/`, runtime `ReplayFiles`).
    replay_file: Option<String>,
    /// Frames shown in this combat, and the moves and turns sent (the once-a-second trace).
    frames: u64,
    moves: u64,
    turns: u64,
    /// The `moving` flag census: (flag set, moved), (flag set, still), (flag clear, moved).
    flag_moved: u64,
    flag_still: u64,
    noflag_moved: u64,
    /// The player's last reported spot and facing (free mode), and the lineup's units.
    me_at: Option<([f32; 3], f32)>,
    lineup: Vec<u64>,
    /// `.lineup mobs <name…>`'s names (lower case); empty for all.
    lineup_only: Vec<String>,
    /// Creature template entry → name, answered to `CreatureQuery`.
    creature_names: HashMap<u32, String>,
    /// Dressed units ([`Self::dress_create`]): their names (`NameQuery`), the item templates their
    /// gear points at (`ItemQuery`), and the next guid (a player's high guid, 0, so names resolve
    /// as a player's).
    player_names: HashMap<u64, String>,
    dress_items: HashMap<u32, (u32, u32, u32, u32)>,
    next_dressed: u64,
}

impl Bridge {
    fn new(events: Sender<SessionEvent>, out: Sender<String>) -> Self {
        Self {
            events,
            out,
            seq: 0,
            realm: RealmInfo {
                name: "Exile of Azeroth".into(),
                address: "127.0.0.1:7710".into(),
                population: 0.0,
                characters: 0,
                realm_type: 0,
                flags: 0,
                category: 1,
                id: 1,
            },
            epoch: 0,
            generation: 0,
            roster: Vec::new(),
            dqq_ids: HashMap::new(),
            self_guid: None,
            me: None,
            shown: HashMap::new(),
            next_creature: 0xF130_0000_0000_0001,
            combat: None,
            models: ModelBook::load(),
            phase: Phase::Free,
            focus: None,
            combat_due: None,
            clear_due: None,
            spline_id: 0,
            ground: Ground::load(BATTLE_CENTER),
            replay_id: None,
            engaged: HashMap::new(),
            ready_first: None,
            me_at: None,
            lineup: Vec::new(),
            lineup_only: Vec::new(),
            creature_names: HashMap::new(),
            player_names: HashMap::new(),
            dress_items: HashMap::new(),
            next_dressed: 0x0010_0000,
            ready_quiet: None,
            ready_sent: true,
            replay_file: None,
            frames: 0,
            moves: 0,
            turns: 0,
            flag_moved: 0,
            flag_still: 0,
            noflag_moved: 0,
        }
    }

    /// DQQ battlefield `(x, y)` (double, 0..10) → WoW world coordinates centred on
    /// [`BATTLE_CENTER`], standing on the terrain there.
    fn to_world(&self, x: f64, y: f64) -> [f32; 3] {
        self.ground.on_ground([
            BATTLE_CENTER[0] + (x - DQQ_FIELD_HALF) as f32 * YARDS_PER_UNIT,
            BATTLE_CENTER[1] + (y - DQQ_FIELD_HALF) as f32 * YARDS_PER_UNIT,
            BATTLE_CENTER[2],
        ])
    }

    fn emit(&self, ev: SessionEvent) -> bool {
        self.events.send(ev).is_ok()
    }

    /// Sends one bridge message to the runtime (`docs/bridge-protocol.md`).
    fn send(&mut self, kind: &str, data: Value) {
        self.seq += 1;
        let msg = json!({ "v": 1, "seq": self.seq, "type": kind, "data": data });
        let _ = self.out.send(msg.to_string());
    }

    fn run(
        &mut self,
        login_rx: &Receiver<LoginRequest>,
        park_rx: &Receiver<ParkRequest>,
        cmd_rx: &Receiver<ClientCommand>,
        ws_rx: &Receiver<Value>,
    ) {
        loop {
            select! {
                recv(login_rx) -> m => match m {
                    Ok(req) => self.on_login(&req),
                    Err(_) => return,
                },
                recv(park_rx) -> m => match m {
                    Ok(p) => self.on_park(p),
                    Err(_) => return,
                },
                recv(cmd_rx) -> m => match m {
                    Ok(cmd) => self.on_command(&cmd),
                    Err(_) => return,
                },
                recv(ws_rx) -> m => match m {
                    Ok(v) => self.on_runtime(&v),
                    Err(_) => {
                        let _ = self.events.send(SessionEvent::Disconnected {
                            reason: "Exile runtime closed".into(),
                            end: benilla_protocol::SessionEnd::Lost,
                        });
                        return;
                    }
                },
                default(Duration::from_millis(20)) => {}
            }
            // Every pass, so a steady stream of messages never starves them.
            self.timers();
        }
    }

    /// The player's commands: in free mode the stock client handles them all; `.replay` typed in
    /// chat starts a combat replay (a stand-in until the addon UI). During a replay nothing the
    /// player does reaches the combat (D29).
    fn on_command(&mut self, cmd: &ClientCommand) {
        match cmd {
            ClientCommand::Chat { text, .. } if text.trim_start().starts_with(".replay") => {
                self.start_replay();
            }
            // `.lineup <display>…`: a review row of models (PLAN-0006).
            ClientCommand::Chat { text, .. } if text.trim_start().starts_with(".lineup") => {
                let args = text.trim_start().trim_start_matches(".lineup").to_owned();
                self.lineup(&args);
            }
            // Where the player stands, for the lineup.
            ClientCommand::Move { pos, orientation, .. } => {
                self.me_at = Some((*pos, *orientation));
            }
            // A unit's name: the entries this bridge filed (`creature_names`); any other unknown.
            ClientCommand::ItemQuery { entry, .. } => {
                let info = self.dress_items.get(entry).map(|&(d, inv, c, sc)| Box::new(item_info(d, inv, c, sc)));
                self.emit(SessionEvent::ItemTemplate { entry: *entry, info });
            }
            ClientCommand::CreatureQuery { entry, .. } => {
                let name = self.creature_names.get(entry).cloned();
                self.emit(creature_name(*entry, name));
            }
            // Names: the player's own (addon lines name it as their sender); any other is unknown.
            ClientCommand::NameQuery { guid } => self.answer_name(*guid),
            // The Exile UI addon's requests (`docs/bridge-protocol.md` §4).
            ClientCommand::AddonMessage { text, .. } => {
                if let Some(msg) = text.strip_prefix(ADDON_PREFIX).and_then(|t| t.strip_prefix('\t')) {
                    self.on_addon(msg);
                }
            }
            // The worldport's load is done.
            ClientCommand::WorldportAck => match self.phase {
                Phase::Porting => {
                    // A worldport clears the world; the player's own unit comes again, as a server
                    // re-sends it after the port.
                    self.create_self(self.ground.on_ground(BATTLE_CENTER), COMBAT_WALK_SPEED);
                    let id = self.self_guid.and_then(|g| self.dqq_ids.get(&g).cloned());
                    let replay = self.replay_id.take();
                    let file = self.replay_file.take();
                    self.send(
                        "combat.replay",
                        json!({
                            "replayId": replay,
                            "file": file,
                            "characterId": id,
                            "mode": "watch",
                            "hold": true,
                        }),
                    );
                    self.phase = Phase::Combat;
                }
                Phase::Returning => {
                    self.create_self(ORIGIN, FREE_WALK_SPEED);
                    if let Some(me) = self.self_guid {
                        self.emit(SessionEvent::ClientControl { mover: me, allow_move: true });
                    }
                    super::set_exile_spectating(false);
                    self.phase = Phase::Free;
                }
                _ => {}
            },
            _ => {}
        }
    }

    /// `.lineup [size=<n>] <display>…` in free mode: the given creature displays in rows of
    /// [`LINEUP_ROW`] before the player, each named `#<display>` and drawn at combat scale for a DQQ
    /// size `n` (default 1). `.lineup mobs [name…]` shows the runtime's monster table instead
    /// (only the named ones, when any are named)
    /// (`ui.mapping.mobs`), each under its DQQ name. A bare `.lineup` clears the row. A review aid
    /// for the DQQ → WoW tables (PLAN-0006).
    fn lineup(&mut self, args: &str) {
        let mut words = args.split_whitespace();
        if words.clone().next() == Some("mobs") {
            // `.lineup mobs [name…]`: only the named monsters, when any are named.
            self.lineup_only = words.skip(1).map(str::to_ascii_lowercase).collect();
            self.send("ui.mapping.mobs", Value::Null);
            return;
        }
        let mut size = 1.0_f32;
        let mut shown = Vec::new();
        for word in args.split(|c: char| c.is_whitespace() || c == ',') {
            if let Some(n) = word.strip_prefix("size=").and_then(|n| n.parse().ok()) {
                size = n;
            } else if let Ok(id) = word.parse::<u32>() {
                shown.push((id, format!("#{id}"), size, None));
            }
        }
        self.lineup_show(shown);
    }

    /// `ui.mapping.mobs.result`: every monster's look, a boss at a boss's size (radius 1.5).
    fn lineup_mobs(&mut self, data: &Value) {
        let shown = data
            .as_array()
            .into_iter()
            .flatten()
            .filter(|m| {
                let name = m["mob"].as_str().unwrap_or_default().to_ascii_lowercase();
                self.lineup_only.is_empty() || self.lineup_only.contains(&name)
            })
            .filter_map(|m| {
                let display = u32::try_from(m["displayId"].as_u64()?).ok()?;
                let size = if m["boss"].as_bool() == Some(true) { 3.0 } else { 1.0 };
                let dress = Some(m["dress"].clone()).filter(Value::is_object);
                Some((display, m["mob"].as_str()?.to_owned(), size, dress))
            })
            .collect();
        self.lineup_show(shown);
    }

    /// The lineup itself: `(display, name, size, dress)` in rows facing the player.
    fn lineup_show(&mut self, shown: Vec<(u32, String, f32, Option<Value>)>) {
        for guid in std::mem::take(&mut self.lineup) {
            self.emit(SessionEvent::ObjectDestroyed(guid));
        }
        if self.phase != Phase::Free {
            return;
        }
        let (at, o) = self.me_at.unwrap_or((ORIGIN, 0.0));
        let (fx, fy) = (o.cos(), o.sin());
        let (rx, ry) = (fy, -fx);
        let widest = shown.iter().map(|s| s.2).fold(1.0_f32, f32::max);
        let gap = LINEUP_GAP * widest.min(2.0);
        for (i, (display, name, size, dress)) in shown.into_iter().enumerate() {
            let (row, col) = ((i / LINEUP_ROW) as f32, (i % LINEUP_ROW) as f32);
            let ahead = 6.0 + row * gap;
            let side = (col - (LINEUP_ROW as f32 - 1.0) / 2.0) * gap;
            let pos = self.ground.on_ground([
                at[0] + fx * ahead + rx * side,
                at[1] + fy * ahead + ry * side,
                at[2],
            ]);
            let facing = (o + std::f32::consts::PI).rem_euclid(std::f32::consts::TAU);
            if let Some(dress) = dress {
                let guid = self.dress_create(&dress, name, size, pos, facing, FREE_WALK_SPEED, &[]);
                self.lineup.push(guid);
                continue;
            }
            let guid = self.next_creature;
            self.next_creature += 1;
            // Its own entry, so each name stands alone; the name goes ahead of the unit, whose
            // create reads the cache.
            let entry = LINEUP_ENTRY + i as u32;
            self.creature_names.insert(entry, name.clone());
            self.emit(creature_name(entry, Some(name)));
            let scale = self.models.unit_scale(display, size, false);
            self.emit(unit_create(
                FREE_WALK_SPEED,
                scale,
                guid,
                EntityKind::Unit,
                display,
                pos,
                facing,
                &[
                    (FIELD_OBJECT_ENTRY, entry),
                    (FIELD_UNIT_HEALTH, 100),
                    (FIELD_UNIT_MAXHEALTH, 100),
                    (FIELD_UNIT_LEVEL, 1),
                    (FIELD_UNIT_FACTIONTEMPLATE, FACTION_HOSTILE),
                ],
            ));
            self.lineup.push(guid);
        }
        bevy::log::info!("exile: lineup of {}", self.lineup.len());
    }

    /// A dressed humanoid (an actor's or a lineup entry's `dress`, runtime `Dress`): a player-style
    /// unit of the given race and sex, its look bytes, and its gear as visible items whose
    /// templates (`ItemTemplate`, display id and slot family) and name (`PlayerName`) go ahead of
    /// it. Drawn at combat scale for DQQ `size`; `pairs` are its other fields. The guid made.
    #[allow(clippy::too_many_arguments)]
    fn dress_create(
        &mut self,
        dress: &Value,
        name: String,
        size: f32,
        pos: [f32; 3],
        facing: f32,
        walk: f32,
        pairs: &[(u16, u32)],
    ) -> u64 {
        let n = |k: &str| u32::try_from(dress[k].as_u64().unwrap_or(0)).unwrap_or(0);
        let (race, gender) = (n("race").max(1), n("gender").min(1));
        let display = native_display(race, gender);
        let guid = self.next_dressed;
        self.next_dressed += 1;
        let mut fields = vec![
            (FIELD_UNIT_BYTES_0, race | 1 << 8 | gender << 16 | 1 << 24),
            (
                FIELD_PLAYER_BYTES,
                n("skin") | n("face") << 8 | n("hair") << 16 | n("hairColor") << 24,
            ),
            (FIELD_PLAYER_BYTES_2, 0),
        ];
        for item in dress["items"].as_array().into_iter().flatten() {
            let m = |k: &str| u32::try_from(item[k].as_u64().unwrap_or(0)).unwrap_or(0);
            let (slot, shown) = (m("slot"), m("displayId"));
            if slot > 18 || shown == 0 {
                continue;
            }
            let entry = DRESS_ITEM_BASE + shown;
            let template = (shown, m("inventoryType"), m("itemClass"), m("itemSubclass"));
            if self.dress_items.insert(entry, template) != Some(template) {
                self.emit(SessionEvent::ItemTemplate {
                    entry,
                    info: Some(Box::new(item_info(template.0, template.1, template.2, template.3))),
                });
            }
            fields.push((FIELD_PLAYER_VISIBLE_ITEM_1_0 + VISIBLE_ITEM_STRIDE * slot as u16, entry));
        }
        fields.extend_from_slice(pairs);
        if !fields.iter().any(|(f, _)| *f == FIELD_UNIT_HEALTH) {
            fields.extend_from_slice(&[(FIELD_UNIT_HEALTH, 100), (FIELD_UNIT_MAXHEALTH, 100)]);
        }
        if !fields.iter().any(|(f, _)| *f == FIELD_UNIT_FACTIONTEMPLATE) {
            fields.push((FIELD_UNIT_FACTIONTEMPLATE, FACTION_HOSTILE));
        }
        if !fields.iter().any(|(f, _)| *f == FIELD_UNIT_LEVEL) {
            fields.push((FIELD_UNIT_LEVEL, 1));
        }
        self.player_names.insert(guid, name.clone());
        self.emit(SessionEvent::PlayerName { guid, name, race, gender, class: 1 });
        // A humanoid like the hero: its own scale times the size, without the footprint cap (a
        // character model's bounds take in its whole animation range and would shrink it).
        let capped = self.models.unit_scale(display, size, false);
        let scale = self.models.unit_scale(display, size, true);
        let body = display;
        bevy::log::info!("exile: dressed {guid:#x} race {race} body {body} scale {scale} (capped {capped})");
        self.emit(unit_create(walk, scale, guid, EntityKind::Player, display, pos, facing, &fields));
        guid
    }

    /// `SMSG_NAME_QUERY_RESPONSE`'s stand-in: our own name, an empty one for anyone else.
    fn answer_name(&mut self, guid: u64) {
        if let Some(name) = self.player_names.get(&guid).cloned() {
            self.emit(SessionEvent::PlayerName { guid, name, race: 0, gender: 0, class: 1 });
            return;
        }
        let me = self.me.clone().filter(|_| Some(guid) == self.self_guid);
        self.emit(SessionEvent::PlayerName {
            guid,
            name: me.as_ref().map(|m| m.name.clone()).unwrap_or_default(),
            race: me.as_ref().map_or(0, |m| u32::from(m.race)),
            gender: me.as_ref().map_or(0, |m| u32::from(m.gender)),
            class: me.as_ref().map_or(0, |m| u32::from(m.class)),
        });
    }

    /// One addon request, `verb;arg;…` (`;` because `|` is the chat escape character).
    fn on_addon(&mut self, msg: &str) {
        bevy::log::info!("exile: addon says {msg:?}");
        let mut parts = msg.split(';');
        match parts.next().unwrap_or_default() {
            // The round trip's test: answered by the bridge itself.
            "ping" => self.addon_say(&format!("pong;{}", parts.next().unwrap_or_default())),
            // The character's recordings, newest first: asked of the runtime (`ui.replay.list`).
            "replay.list" => {
                let id = self.self_guid.and_then(|g| self.dqq_ids.get(&g).cloned());
                self.send("ui.replay.list", json!({ "characterId": id }));
            }
            // Play one recording (an empty id: the latest).
            "replay.play" => {
                if self.phase != Phase::Free {
                    self.addon_say("error;A replay is already playing.");
                    return;
                }
                self.replay_id = parts.next().filter(|id| !id.is_empty()).map(str::to_owned);
                self.replay_file = None;
                self.start_replay();
            }
            // Export one recording to a file (`ui.replay.export`, the DQQ export format).
            "replay.export" => {
                let id = parts.next().unwrap_or_default();
                self.send("ui.replay.export", json!({ "replayId": id }));
            }
            // The recording files (`ui.replay.files`).
            "replay.files" => self.send("ui.replay.files", Value::Null),
            // Play a recording file.
            "replay.file" => {
                if self.phase != Phase::Free {
                    self.addon_say("error;A replay is already playing.");
                    return;
                }
                let Some(file) = parts.next().filter(|f| !f.is_empty()) else { return };
                self.replay_file = Some(file.to_owned());
                self.replay_id = None;
                self.start_replay();
            }
            other => bevy::log::warn!("exile: unknown addon request {other:?}"),
        }
    }

    /// A line to the Exile UI addon: an addon message (`LANG_ADDON`) on the party lane, sent by
    /// the player (`CHAT_MSG_ADDON` waits on its sender's name; [`Self::answer_name`]).
    fn addon_say(&mut self, msg: &str) {
        bevy::log::debug!("exile: to the addon {msg:?}");
        self.emit(SessionEvent::Chat(ChatMessage {
            chat_type: CHAT_MSG_PARTY,
            language: LANGUAGE_ADDON,
            sender_guid: self.self_guid.unwrap_or(0),
            target_guid: 0,
            sender_name: None,
            channel: None,
            text: format!("{ADDON_PREFIX}\t{msg}"),
            chat_tag: 0,
        }));
    }

    /// `ui.replay.list.result` for the addon: `replay.begin;<n>`, one `replay;<id>;<label>` each
    /// (at most [`ADDON_REPLAYS`]), then `replay.end`.
    fn replays_to_addon(&mut self, list: &Value) {
        self.list_to_addon("replay", "id", list);
    }

    /// `ui.replay.files.result` for the addon: `file.begin;<n>`, `file;<name>;<label>`, `file.end`.
    fn files_to_addon(&mut self, list: &Value) {
        self.list_to_addon("file", "file", list);
    }

    fn list_to_addon(&mut self, verb: &str, key: &str, list: &Value) {
        let all = list.as_array().cloned().unwrap_or_default();
        bevy::log::info!("exile: {} {verb} entries for the addon", all.len());
        self.addon_say(&format!("{verb}.begin;{}", all.len()));
        for r in all.iter().take(ADDON_REPLAYS) {
            let key = r[key].as_str().unwrap_or_default().replace([';', '|'], " ");
            let label = if verb == "file" {
                // The file's own name leads: "dqq-replay-c6bc687b.txt" shows as "c6bc687b".
                let short = key.trim_start_matches("dqq-replay-").trim_end_matches(".txt");
                format!("{short}  {}", replay_label(r))
            } else {
                replay_label(r)
            };
            self.addon_say(&format!("{verb};{key};{label}"));
        }
        self.addon_say(&format!("{verb}.end"));
    }

    /// D28: control taken and the port to the battlefield, whose loading screen is the "bar"; the
    /// replay starts on the port's ack ([`Self::on_command`]).
    fn start_replay(&mut self) {
        let Some(me) = self.self_guid else { return };
        if self.phase != Phase::Free {
            bevy::log::info!("exile: a replay is already under way");
            return;
        }
        // Free mode's NPCs and interactables would be hidden here (none are spawned yet).
        self.emit(SessionEvent::ClientControl { mover: me, allow_move: false });
        super::set_exile_spectating(true);
        self.emit(SessionEvent::Worldport {
            map_id: MAP_ID,
            position: self.ground.on_ground(BATTLE_CENTER),
            orientation: 0.0,
            needs_ack: true,
        });
        self.phase = Phase::Porting;
    }

    /// The delayed auto replay, and the clear and the way home after a combat.
    fn timers(&mut self) {
        let now = Instant::now();
        self.ready_check(now);
        if self.combat_due.is_some_and(|t| now >= t) {
            self.combat_due = None;
            self.start_replay();
        }
        let me = self.self_guid;
        if self.clear_due.is_some_and(|t| now >= t) {
            self.clear_due = None;
            // The hero leaves the combat stance before the way home.
            if let Some((me, victim)) = me.and_then(|m| self.engaged.get(&m).map(|v| (m, *v))) {
                self.emit(SessionEvent::AttackStop { attacker: me, victim });
            }
            self.engaged.clear();
            let guids: Vec<u64> = self.shown.drain().map(|(_, s)| s.guid).filter(|g| Some(*g) != me).collect();
            for guid in guids {
                self.emit(SessionEvent::ObjectDestroyed(guid));
            }
            // Back to free mode: home; the controls come back on the port's ack.
            if me.is_some() {
                self.emit(SessionEvent::Worldport {
                    map_id: MAP_ID,
                    position: ORIGIN,
                    orientation: 0.0,
                    needs_ack: true,
                });
            }
            self.focus = None;
            self.phase = Phase::Returning;
        }
    }

    /// Every unit in a combat holds the combat stance (user, 2026-11-07): engaged, as an attack
    /// start leaves it (`SMSG_ATTACKSTART`), so it idles in the Ready pose, not the peaceful stand.
    /// An enemy engages the nearest of the hero's side, the hero's side the nearest enemy; a unit
    /// keeps its victim while that lives.
    fn keep_engaged(&mut self) {
        let units: Vec<(u64, [f32; 3], bool)> =
            self.shown.values().map(|s| (s.guid, s.pos, s.enemy)).collect();
        for &(guid, pos, enemy) in &units {
            if self.engaged.get(&guid).is_some_and(|v| units.iter().any(|u| u.0 == *v)) {
                continue;
            }
            let nearest = units
                .iter()
                .filter(|u| u.2 != enemy)
                .min_by(|a, b| {
                    let da = (a.1[0] - pos[0]).hypot(a.1[1] - pos[1]);
                    let db = (b.1[0] - pos[0]).hypot(b.1[1] - pos[1]);
                    da.total_cmp(&db)
                })
                .map(|u| u.0);
            if let Some(victim) = nearest {
                self.engaged.insert(guid, victim);
                self.emit(SessionEvent::AttackStart { attacker: guid, victim });
            }
        }
    }

    /// The combat's start hold (user, 2026-11-07): the first frame's units are created, then the
    /// runtime waits until every model is built (and [`READY_SETTLE`] more, for the fade-in),
    /// at least [`READY_MIN`] and at most [`READY_MAX`]; `combat.ready` lets it run.
    fn ready_check(&mut self, now: Instant) {
        if self.ready_sent {
            return;
        }
        let Some(first) = self.ready_first else { return };
        let pending = UNEMBODIED.load(Ordering::Relaxed);
        if pending == 0 {
            self.ready_quiet.get_or_insert(now);
        } else {
            self.ready_quiet = None;
        }
        let settled = self.ready_quiet.is_some_and(|q| now - q >= READY_SETTLE);
        let waited = now - first;
        if (settled && waited >= READY_MIN) || waited >= READY_MAX {
            bevy::log::info!(
                "exile: combat ready after {:.1}s ({pending} models still loading)",
                waited.as_secs_f32()
            );
            self.ready_sent = true;
            let id = self.combat.clone();
            self.send("combat.ready", json!({ "combatId": id }));
        }
    }

    /// The env login: no realmd, so the logon succeeds at once and the one realm is listed.
    fn on_login(&mut self, _req: &LoginRequest) {
        for stage in [
            LoginStage::Connecting,
            LoginStage::Authenticating,
            LoginStage::CheckingVersions,
            LoginStage::ProofSent,
        ] {
            self.emit(SessionEvent::LoginStage { stage });
        }
        self.emit(SessionEvent::RealmList {
            realms: vec![self.realm.clone()],
            failed: false,
            emptied: None,
        });
    }

    fn on_park(&mut self, park: ParkRequest) {
        match park {
            ParkRequest::Realm(RealmRequest::Enter { generation, .. }) => {
                self.generation = generation;
                self.epoch += 1;
                self.emit(SessionEvent::LoginStage { stage: LoginStage::Handshaking });
                self.emit(SessionEvent::LoginStage { stage: LoginStage::WorldConnected });
                self.emit(SessionEvent::WorldAuthenticated {
                    realm: self.realm.clone(),
                    epoch: self.epoch,
                    generation,
                });
            }
            ParkRequest::Realm(RealmRequest::Refresh) => {
                self.emit(SessionEvent::RealmList {
                    realms: vec![self.realm.clone()],
                    failed: false,
                    emptied: None,
                });
            }
            ParkRequest::Char(CharRequest::Enumerate) => self.send("ui.character.list", Value::Null),
            ParkRequest::Char(CharRequest::Enter(guid)) => self.enter_world(guid),
            _ => bevy::log::info!("exile: park request not handled in Exile mode"),
        }
    }

    fn on_runtime(&mut self, msg: &Value) {
        let kind = msg["type"].as_str().unwrap_or_default();
        let data = &msg["data"];
        match kind {
            "hello" => bevy::log::info!("exile: runtime says hello {data}"),
            "ui.character.list.result" => self.roster_from(data),
            "ui.mapping.mobs.result" => self.lineup_mobs(data),
            "ui.replay.list.result" => self.replays_to_addon(data),
            "ui.replay.files.result" => self.files_to_addon(data),
            "ui.replay.export.result" => {
                let file = data["file"].as_str().unwrap_or_default().replace([';', '|'], " ");
                bevy::log::info!("exile: exported {}", data["path"]);
                self.addon_say(&format!("exported;{file}"));
            }
            "combat.begin" => {
                self.combat = data["combatId"].as_str().map(str::to_owned);
                (self.frames, self.moves, self.turns) = (0, 0, 0);
                (self.ready_first, self.ready_quiet, self.ready_sent) = (None, None, false);
                (self.flag_moved, self.flag_still, self.noflag_moved) = (0, 0, 0);
                self.focus = data["focusId"].as_str().map(str::to_owned);
                bevy::log::info!("exile: combat begins {data}");
            }
            "combat.frame" => {
                self.frame(data);
                // The start's frames: the runtime holds after them until the models are up
                // ([`Self::ready_check`]).
                if !self.ready_sent {
                    self.ready_first.get_or_insert_with(Instant::now);
                }
            }
            "combat.end" => {
                bevy::log::info!(
                    "exile: moving flag census: set+moved {}, set+still {}, clear+moved {}",
                    self.flag_moved,
                    self.flag_still,
                    self.noflag_moved
                );
                bevy::log::info!(
                    "exile: combat ends after {} frames, {} moves, {} turns sent {data}",
                    self.frames,
                    self.moves,
                    self.turns
                );
                self.combat = None;
                self.clear_due = Some(Instant::now() + CLEAR_DELAY);
            }
            "error" => {
                bevy::log::error!("exile: runtime error {data}");
                let text = data["message"].as_str().unwrap_or("runtime error").replace([';', '|'], " ");
                self.addon_say(&format!("error;{text}"));
            }
            _ => {}
        }
    }

    /// The runtime's characters as the select screen's roster (D20: human female by default).
    fn roster_from(&mut self, list: &Value) {
        self.roster.clear();
        self.dqq_ids.clear();
        for (i, c) in list.as_array().into_iter().flatten().enumerate() {
            let guid = i as u64 + 1;
            self.dqq_ids.insert(guid, c["id"].as_str().unwrap_or_default().to_owned());
            self.roster.push(Character {
                guid,
                name: c["name"].as_str().unwrap_or("Exile").chars().take(12).collect(),
                race: c["wowRace"].as_u64().unwrap_or(1) as u8,
                class: c["wowClass"].as_u64().unwrap_or(1) as u8,
                gender: c["wowGender"].as_u64().unwrap_or(1) as u8,
                skin: 0,
                face: 0,
                hair_style: 0,
                hair_color: 0,
                facial_hair: 0,
                level: c["level"].as_u64().unwrap_or(1).clamp(1, 60) as u8,
                new_character: false,
                zone: 9, // Northshire Valley
                map: MAP_ID,
                position: benilla_protocol::wire::Vector3d {
                    x: ORIGIN[0],
                    y: ORIGIN[1],
                    z: ORIGIN[2],
                },
                guild_id: 0,
                flags: 0,
                equipment: [CharEnumItem::default(); 19],
                pet_display_id: 0,
                pet_level: 0,
                pet_family: 0,
            });
        }
        self.emit(SessionEvent::CharacterList {
            characters: self.roster.clone(),
            realm: Some(self.realm.clone()),
            cause: RosterCause::Asked,
            epoch: self.epoch,
            generation: self.generation,
        });
    }

    /// Entering the world as roster entry `guid`: the session starts, the login position, and the
    /// player's own unit: free mode (D27). `EXILE_AUTOCOMBAT=1` (unattended tests) starts the replay
    /// trip by itself after [`AUTOCOMBAT_DELAY`].
    fn enter_world(&mut self, guid: u64) {
        let Some(me) = self.roster.iter().find(|c| c.guid == guid).cloned() else {
            bevy::log::error!("exile: no roster entry {guid}");
            return;
        };
        self.self_guid = Some(guid);
        self.emit(SessionEvent::Connected {
            self_guid: guid,
            name: me.name.clone(),
            billing: Billing::default(),
            tutorial_flags: Some(vec![0xFF; 32]),
            addon_info: Some(Vec::new()),
        });
        self.emit(SessionEvent::Worldport {
            map_id: MAP_ID,
            position: ORIGIN,
            orientation: 0.0,
            needs_ack: false,
        });
        // The account-data digests a server sends after the login verify: all empty, so every
        // section settles from this machine's files or the defaults. Without them the sections stay
        // pending and the key bindings never load (no WASD, mouse turn or Escape).
        self.emit(SessionEvent::AccountDataMd5([[0u8; 16]; ACCOUNT_DATA_SECTIONS]));
        self.me = Some(me);
        self.answer_name(guid);
        self.create_self(ORIGIN, FREE_WALK_SPEED);
        if std::env::var("EXILE_AUTOCOMBAT").as_deref() == Ok("1") {
            self.combat_due = Some(Instant::now() + AUTOCOMBAT_DELAY);
        }
    }

    /// The player's own unit at `pos`: at world entry and again after every worldport.
    fn create_self(&mut self, pos: [f32; 3], walk: f32) {
        let (Some(guid), Some(me)) = (self.self_guid, self.me.clone()) else { return };
        let bytes0 = me.race as u32 | (me.class as u32) << 8 | (me.gender as u32) << 16;
        self.emit(unit_create(
            walk,
            1.0,
            guid,
            EntityKind::Player,
            50,
            pos,
            0.0,
            &[
                (FIELD_UNIT_HEALTH, 100),
                (FIELD_UNIT_MAXHEALTH, 100),
                (FIELD_UNIT_LEVEL, me.level as u32),
                (FIELD_UNIT_FACTIONTEMPLATE, FACTION_PLAYER),
                (FIELD_UNIT_BYTES_0, bytes0),
            ],
        ));
    }

    /// One `combat.frame`: create the actors not yet shown, walk the rest to their new spot
    /// (`MonsterMove`, so the client animates the walk), update health, remove those gone, then play
    /// the frame's damage as melee swings with floating numbers (`AttackerState`). The hero is a
    /// unit of its own, apart from the (hidden) player (§5.1).
    fn frame(&mut self, data: &Value) {
        self.frames += 1;
        let trace = self.frames % 30 == 1;
        let mut seen = Vec::new();
        for actor in data["actors"].as_array().into_iter().flatten() {
            let Some(id) = actor["id"].as_str() else { continue };
            seen.push(id.to_owned());
            let (x, y) = (actor["x"].as_f64().unwrap_or(0.0), actor["y"].as_f64().unwrap_or(0.0));
            let pos = self.to_world(x, y);
            // DQQ keeps no facing for actors (their `angle` stays 0): they face where they walk,
            // or, standing, their target (`targetX`/`targetY`).
            let target = match (actor["targetX"].as_f64(), actor["targetY"].as_f64()) {
                (Some(tx), Some(ty)) => Some(self.to_world(tx, ty)),
                _ => None,
            };
            let max_life = actor["maxLife"].as_f64().unwrap_or(100.0).max(1.0).ceil() as u32;
            let life = actor["life"].as_f64().unwrap_or(max_life as f64).max(0.0).ceil() as u32;
            if trace && (self.focus.as_deref() == Some(id) || seen.len() <= 2) {
                bevy::log::info!(
                    "exile: frame {} actor {id} dqq ({x:.3}, {y:.3}) moving {} target {:?} → world ({:.2}, {:.2}, {:.2})",
                    self.frames,
                    actor["moving"],
                    (actor["targetX"].as_f64(), actor["targetY"].as_f64()),
                    pos[0],
                    pos[1],
                    pos[2]
                );
            }
            // The combat's creator is played by the player's own body (D28): first seen, it takes
            // the self guid, so the camera's stock chase follows it.
            if !self.shown.contains_key(id) && self.focus.as_deref() == Some(id) {
                if let Some(me) = self.self_guid {
                    let from = self.ground.on_ground(BATTLE_CENTER);
                    // The hero is the human female standard: her size is her scale.
                    self.emit(SessionEvent::ObjectValues {
                        guid: me,
                        fields: ObjectFields::from_pairs(&[(
                            FIELD_OBJECT_SCALE_X,
                            actor_size(actor).to_bits(),
                        )]),
                    });
                    self.shown.insert(id.to_owned(), Shown {
                            guid: me,
                            pos: from,
                            life,
                            facing: 0.0,
                            walk_until: 0,
                            last_dqq: pos,
                            enemy: false,
                            walk_start: None,
                        });
                }
            }
            if let Some(shown) = self.shown.get(id) {
                let (guid, from, old_life, old_facing) = (shown.guid, shown.pos, shown.life, shown.facing);
                let (walk_until, last_dqq) = (shown.walk_until, shown.last_dqq);
                // The flag census: does DQQ's `moving` agree with the steps?
                let stepped = (pos[0] - last_dqq[0]).hypot(pos[1] - last_dqq[1]) > 0.001;
                match (actor["moving"].as_bool() == Some(true), stepped) {
                    (true, true) => self.flag_moved += 1,
                    (true, false) => self.flag_still += 1,
                    (false, true) => self.noflag_moved += 1,
                    _ => {}
                }
                let mut facing = old_facing;
                let mut shown_at = from;
                let mut until = walk_until;
                let mut started = shown.walk_start;
                let step = (pos[0] - from[0]).hypot(pos[1] - from[1]);
                if step > 0.001 {
                    // A step: sent at once, from where the last one ended. A walk that has just
                    // started has its spline stretched to reach [`SEGMENT_FRAMES`] past its start;
                    // after that each step lasts [`STEP_FRAMES`], so the next one takes over
                    // before it ends and the run cycle never breaks.
                    let start = started.filter(|_| self.frames <= walk_until + 1).unwrap_or(self.frames);
                    started = Some(start);
                    let frames_left = ((start + SEGMENT_FRAMES).saturating_sub(self.frames) as f32)
                        .max(STEP_FRAMES);
                    facing = wow_facing(pos[0] - from[0], pos[1] - from[1]);
                    self.spline_id = self.spline_id.wrapping_add(1);
                    self.moves += 1;
                    self.emit(SessionEvent::MonsterMove {
                        guid,
                        transport: None,
                        start: from,
                        spline_id: self.spline_id,
                        // A path starts at the wire's start point, as the packet decodes it: a
                        // one-point path installs no spline (`monster_move_spline`).
                        path: vec![from, pos],
                        facing: MonsterMoveFacing::None,
                        stop: false,
                        duration_ms: (frames_left * TICK_MS).round() as u32,
                        flying: false,
                        run_mode: true,
                    });
                    shown_at = pos;
                    until = self.frames + frames_left.ceil() as u64;
                } else if self.frames >= walk_until {
                    started = None;
                    if let Some(t) = target {
                        // Standing: turn to the target when it is off by more than a few degrees.
                        let (tx, ty) = (t[0] - from[0], t[1] - from[1]);
                        if tx.hypot(ty) > 0.05 {
                            let want = wow_facing(tx, ty);
                            if angle_gap(want, old_facing) > 0.1 {
                                facing = want;
                                self.spline_id = self.spline_id.wrapping_add(1);
                                self.turns += 1;
                                self.emit(SessionEvent::MonsterMove {
                                    guid,
                                    transport: None,
                                    start: from,
                                    spline_id: self.spline_id,
                                    path: Vec::new(),
                                    facing: MonsterMoveFacing::Angle(want),
                                    stop: true,
                                    duration_ms: 0,
                                    flying: false,
                                    run_mode: false,
                                });
                            }
                        }
                    }
                }
                if life != old_life {
                    self.emit(SessionEvent::ObjectValues {
                        guid,
                        fields: ObjectFields::from_pairs(&[(FIELD_UNIT_HEALTH, life)]),
                    });
                }
                if let Some(s) = self.shown.get_mut(id) {
                    s.pos = shown_at;
                    s.life = life;
                    s.facing = facing;
                    s.walk_until = until;
                    s.last_dqq = pos;
                    s.walk_start = started;
                }
            } else {
                let player = actor["kind"].as_str() == Some("player");
                let facing = target
                    .map(|t| (t[0] - pos[0], t[1] - pos[1]))
                    .filter(|(tx, ty)| tx.hypot(*ty) > 0.05)
                    .map_or(0.0, |(tx, ty)| wow_facing(tx, ty));
                let guid = if actor["dress"].is_object() {
                    // A dressed humanoid (runtime `Dress`): a player-style unit in its gear.
                    let name = actor["mob"].as_str().unwrap_or_default().to_owned();
                    let dress = actor["dress"].clone();
                    self.dress_create(
                        &dress,
                        name,
                        actor_size(actor),
                        pos,
                        facing,
                        COMBAT_WALK_SPEED,
                        &[
                            (FIELD_UNIT_HEALTH, life),
                            (FIELD_UNIT_MAXHEALTH, max_life),
                            (FIELD_UNIT_LEVEL, 1),
                            (FIELD_UNIT_FACTIONTEMPLATE, FACTION_HOSTILE),
                        ],
                    )
                } else {
                let guid = self.next_creature;
                self.next_creature += 1;
                let key = actor["model"].as_str().unwrap_or("Kobold").to_owned();
                let display = self.models.resolve(&key);
                let scale = self.models.unit_scale(
                    display,
                    actor_size(actor),
                    actor["oversize"].as_bool() == Some(true),
                );
                self.emit(unit_create(
                    COMBAT_WALK_SPEED,
                    scale,
                    guid,
                    EntityKind::Unit,
                    display,
                    pos,
                    facing,
                    &[
                        (FIELD_UNIT_HEALTH, life),
                        (FIELD_UNIT_MAXHEALTH, max_life),
                        (FIELD_UNIT_LEVEL, 1),
                        (
                            FIELD_UNIT_FACTIONTEMPLATE,
                            if player { FACTION_PLAYER } else { FACTION_HOSTILE },
                        ),
                    ],
                ));
                guid
                };
                self.shown.insert(
                    id.to_owned(),
                    Shown {
                        guid,
                        pos,
                        life,
                        facing,
                        walk_until: 0,
                        last_dqq: pos,
                        enemy: actor["kind"].as_str() == Some("enemy"),
                        walk_start: None,
                    },
                );
            }
        }
        let gone: Vec<String> = self.shown.keys().filter(|k| !seen.contains(k)).cloned().collect();
        for key in gone {
            if let Some(s) = self.shown.remove(&key) {
                self.engaged.remove(&s.guid);
                self.emit(SessionEvent::ObjectDestroyed(s.guid));
            }
        }
        self.keep_engaged();
        for ev in data["events"].as_array().into_iter().flatten() {
            if ev["kind"].as_str() != Some("DamageTaken") || ev["success"].as_bool() != Some(true) {
                continue;
            }
            let guid_of = |k: &str| ev[k].as_str().and_then(|id| self.shown.get(id)).map(|s| s.guid);
            let (Some(attacker), Some(victim)) = (guid_of("from"), guid_of("target")) else {
                continue;
            };
            let crit = ev["critical"].as_bool() == Some(true);
            self.emit(SessionEvent::AttackerState(AttackerState {
                attacker,
                victim,
                // HITINFO_AFFECTS_VICTIM, plus HITINFO_CRITICALHIT.
                hit_info: 0x2 | if crit { 0x80 } else { 0 },
                damage: ev["amount"].as_f64().unwrap_or(0.0).ceil().max(1.0) as u32,
                victim_state: 1,
                absorb: 0,
                resist: 0,
                blocked: 0,
                melee_spell_id: 0,
                school: 0,
            }));
        }
    }
}

/// A recording's one-line label for the addon, led by whose combat it is and the recording's own
/// name when it has one: "1a  11-06 19:50  Win  29 ticks  XP 160  dmg 11692".
fn replay_label(r: &Value) -> String {
    let who = [r["character"].as_str(), r["name"].as_str()]
        .into_iter()
        .flatten()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    // "2026-11-06T19:50:08…" → "11-06 19:50".
    let when = r["playTime"]
        .as_str()
        .and_then(|t| t.get(5..16))
        .map(|t| t.replace('T', " "))
        .unwrap_or_default();
    format!(
        "{who}{}{when}  {}  {} ticks  XP {}  dmg {}",
        if who.is_empty() { "" } else { "  " },
        r["result"].as_str().unwrap_or("?"),
        r["ticks"].as_u64().unwrap_or(0),
        r["experience"].as_i64().unwrap_or(0),
        r["totalDamage"].as_str().unwrap_or("0"),
    )
    .replace([';', '|'], " ")
}

/// A DQQ actor's size: its radius over a standard one (`TileConst.TileRadius` 0.5, a character's
/// and a common monster's), so the human female standard is 1, an elite 2, a boss 3.
fn actor_size(actor: &Value) -> f32 {
    (actor["radius"].as_f64().unwrap_or(STANDARD_RADIUS) / STANDARD_RADIUS).max(0.1) as f32
}

/// DQQ's standard actor radius (`TileConst.TileRadius`), size 1.
const STANDARD_RADIUS: f64 = 0.5;

/// The WoW facing (radians, 0 = +x north, counter-clockwise, in `[0, 2π)`) of a world-space
/// step `(dx, dy)`.
fn wow_facing(dx: f32, dy: f32) -> f32 {
    dy.atan2(dx).rem_euclid(std::f32::consts::TAU)
}

/// The unsigned gap between two facings, radians in `[0, π]`.
fn angle_gap(a: f32, b: f32) -> f32 {
    let d = (a - b).rem_euclid(std::f32::consts::TAU);
    d.min(std::f32::consts::TAU - d)
}

/// A race and sex's own body (`ChrRaces.dbc` male/female display): what a dressed unit wears its
/// gear on.
fn native_display(race: u32, gender: u32) -> u32 {
    let (male, female) = match race {
        1 => (49, 50),
        2 => (51, 52),
        3 => (53, 54),
        4 => (55, 56),
        5 => (57, 58),
        6 => (59, 60),
        7 => (1563, 1564),
        8 => (1478, 1479),
        _ => (49, 50),
    };
    if gender == 1 { female } else { male }
}

/// The item template a dressed unit's gear points at: only what drawing it needs, its
/// `ItemDisplayInfo` id, slot family (`InventoryType`), class and subclass.
fn item_info(display: u32, inventory_type: u32, class: u32, subclass: u32) -> ItemInfo {
    ItemInfo {
        class,
        subclass,
        name: String::new(),
        display_info_id: display,
        quality: 1,
        flags: 0,
        buy_price: 0,
        sell_price: 0,
        inventory_type,
        allowable_class: -1,
        allowable_race: -1,
        item_level: 1,
        required_level: 0,
        required_skill: 0,
        required_skill_rank: 0,
        required_spell: 0,
        required_honor_rank: 0,
        required_city_rank: 0,
        required_rep_faction: 0,
        required_rep_rank: 0,
        max_count: 0,
        stackable: 1,
        container_slots: 0,
        stats: Vec::new(),
        damages: Vec::new(),
        dmg_min: 0.0,
        dmg_max: 0.0,
        dmg_type: 0,
        armor: 0,
        resistances: [0; 6],
        delay_ms: 0,
        ammo_type: 0,
        ranged_mod_range: 0.0,
        spells: Vec::new(),
        spell_charges_0: 0,
        use_spell: None,
        bonding: 0,
        description: String::new(),
        page_text: 0,
        language_id: 0,
        page_material: 0,
        start_quest: 0,
        lock_id: 0,
        material: 0,
        // A two-hander rides on the back, anything else at the side.
        sheath: if inventory_type == 17 { 1 } else { 3 },
        random_property: 0,
        block: 0,
        item_set: 0,
        max_durability: 0,
        area: 0,
        map: 0,
        bag_family: 0,
    }
}

/// `SMSG_CREATURE_QUERY_RESPONSE`'s stand-in: a creature template's name (`None`: unknown).
fn creature_name(entry: u32, name: Option<String>) -> SessionEvent {
    SessionEvent::CreatureName {
        entry,
        name,
        subname: None,
        creature_type: None,
        pet_family: 0,
        rank: 0,
        type_flags: 0,
        pet_spell_list: 0,
        display_id: 0,
        civilian: false,
        racial_leader: false,
    }
}

/// A unit or player create, as the update stream carries it: `OBJECT_FIELD_TYPE`, the display
/// ids and the given fields.
fn unit_create(
    walk: f32,
    scale: f32,
    guid: u64,
    kind: EntityKind,
    display: u32,
    position: [f32; 3],
    orientation: f32,
    pairs: &[(u16, u32)],
) -> SessionEvent {
    let player = kind == EntityKind::Player;
    let as_type = if player { ObjectType::Player } else { ObjectType::Unit };
    let mut all = vec![
        (FIELD_OBJECT_TYPE, if player { 0x19 } else { 0x9 }),
        (FIELD_UNIT_DISPLAYID, display),
        (FIELD_UNIT_NATIVEDISPLAYID, display),
        (FIELD_OBJECT_SCALE_X, scale.to_bits()),
    ];
    all.extend_from_slice(pairs);
    SessionEvent::ObjectCreate {
        guid,
        kind,
        display_id: Some(display),
        position,
        orientation,
        scale,
        speeds: Some(MoveSpeeds {
            walk,
            run: 7.0,
            run_back: 4.5,
            swim: 4.72,
            swim_back: 2.5,
            turn_rate: std::f32::consts::PI,
        }),
        mover: None,
        transport_progress: None,
        transport: None,
        spline: None,
        fields: ObjectFields::from_pairs(&all).into_created(as_type),
    }
}
