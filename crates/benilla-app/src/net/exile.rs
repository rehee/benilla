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

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use benilla_protocol::field::{
    FIELD_UNIT_DISPLAYID, FIELD_UNIT_FACTIONTEMPLATE, FIELD_UNIT_HEALTH, FIELD_UNIT_LEVEL,
    FIELD_UNIT_MAXHEALTH,
};
use benilla_protocol::messages::{AttackerState, Billing, ObjectType, ACCOUNT_DATA_SECTIONS};
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
const FIELD_UNIT_BYTES_0: u16 = 36;
const FIELD_UNIT_NATIVEDISPLAYID: u16 = 132;
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
}

/// A walk is shown in segments of this many frames (user rule, 2026-11-06): a walk that starts
/// plays at least this long; at the segment's end a still unit stops, a moving one goes straight
/// on into the next segment, the walk cycle running on unbroken. The frames are shown this many
/// frames late, so each segment's path is known whole when it starts.
const SEGMENT_FRAMES: usize = 5;
/// The walk speeds units are created with. The client picks Run over Walk above twice the walk
/// speed, and DQQ's units cover about a yard a second, so in combat the walk speed is set so low
/// that every walk shows the run (user, 2026-11-06); free mode keeps the stock 2.5.
const COMBAT_WALK_SPEED: f32 = 0.05;
const FREE_WALK_SPEED: f32 = 2.5;
/// One DQQ tick, milliseconds (30 ticks/s).
const TICK_MS: f32 = 1000.0 / 30.0;
/// A segment shorter than this (yards) is no walk.
const WALK_MIN: f32 = 0.05;

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
/// How long a finished combat's units stay before they are removed.
const CLEAR_DELAY: Duration = Duration::from_secs(3);

/// Model keys from the runtime (`docs/bridge-protocol.md` ActorState `model`) resolved against the
/// install's `CreatureDisplayInfo`: a number is a display id; `@HumanFemale` the first human female
/// character-model NPC; any other key the first textured creature whose model path contains it.
struct ModelBook {
    catalog: Option<benilla_formats::CreatureCatalog>,
    resolved: HashMap<String, u32>,
}

impl ModelBook {
    fn load() -> Self {
        let catalog = benilla_formats::wow_data()
            .and_then(|data| benilla_formats::open_chain(&data).ok())
            .and_then(|mut chain| benilla_formats::load_creature_catalog(&mut chain).ok());
        bevy::log::info!(
            "exile: creature catalog {}",
            catalog.as_ref().map_or(0, benilla_formats::CreatureCatalog::len)
        );
        Self { catalog, resolved: HashMap::new() }
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
    /// Frames shown in this combat, and the moves and turns sent (the once-a-second trace).
    frames: u64,
    moves: u64,
    turns: u64,
    /// The last [`SEGMENT_FRAMES`] frames received, not yet shown: the lookahead of the walk
    /// segments.
    pending: VecDeque<Value>,
    /// The `moving` flag census: (flag set, moved), (flag set, still), (flag clear, moved).
    flag_moved: u64,
    flag_still: u64,
    noflag_moved: u64,
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
            frames: 0,
            moves: 0,
            turns: 0,
            pending: VecDeque::new(),
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
                default(Duration::from_millis(50)) => self.timers(),
            }
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
            // The worldport's load is done.
            ClientCommand::WorldportAck => match self.phase {
                Phase::Porting => {
                    // A worldport clears the world; the player's own unit comes again, as a server
                    // re-sends it after the port.
                    self.create_self(self.ground.on_ground(BATTLE_CENTER), COMBAT_WALK_SPEED);
                    let id = self.self_guid.and_then(|g| self.dqq_ids.get(&g).cloned());
                    self.send("combat.replay", json!({ "characterId": id, "mode": "watch" }));
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
        if self.combat_due.is_some_and(|t| now >= t) {
            self.combat_due = None;
            self.start_replay();
        }
        let me = self.self_guid;
        if self.clear_due.is_some_and(|t| now >= t) {
            self.clear_due = None;
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
            "combat.begin" => {
                self.combat = data["combatId"].as_str().map(str::to_owned);
                (self.frames, self.moves, self.turns) = (0, 0, 0);
                (self.flag_moved, self.flag_still, self.noflag_moved) = (0, 0, 0);
                self.pending.clear();
                self.focus = data["focusId"].as_str().map(str::to_owned);
                bevy::log::info!("exile: combat begins {data}");
            }
            "combat.frame" => {
                self.pending.push_back(data.clone());
                while self.pending.len() > SEGMENT_FRAMES {
                    if let Some(f) = self.pending.pop_front() {
                        self.frame(&f);
                    }
                }
            }
            "combat.end" => {
                // The frames still held back play out now, their walks shortened to what is left.
                while let Some(f) = self.pending.pop_front() {
                    self.frame(&f);
                }
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
            "error" => bevy::log::error!("exile: runtime error {data}"),
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
                    self.shown.insert(id.to_owned(), Shown {
                            guid: me,
                            pos: from,
                            life,
                            facing: 0.0,
                            walk_until: 0,
                            last_dqq: pos,
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
                if self.frames >= walk_until {
                    // No segment under way: the next frames' path decides walk or stand.
                    let mut path = vec![from];
                    for f in &self.pending {
                        let Some(next) = f["actors"]
                            .as_array()
                            .and_then(|a| a.iter().find(|a| a["id"].as_str() == Some(id)))
                        else {
                            break;
                        };
                        let p = self.to_world(
                            next["x"].as_f64().unwrap_or(0.0),
                            next["y"].as_f64().unwrap_or(0.0),
                        );
                        path.push(p);
                    }
                    // This frame's own spot leads when no lookahead is left (the combat's end).
                    if path.len() == 1 {
                        path.push(pos);
                    }
                    let steps = path.len() - 1;
                    let length: f32 = path
                        .windows(2)
                        .map(|w| (w[1][0] - w[0][0]).hypot(w[1][1] - w[0][1]))
                        .sum();
                    let end = path[steps];
                    if length > WALK_MIN {
                        // A segment: the whole stretch at its average speed, the unit facing its
                        // way. Back to back, one segment's end is the next one's start, so the
                        // walk runs on without restarting.
                        path.dedup_by(|b, a| (b[0] - a[0]).hypot(b[1] - a[1]) < 0.001);
                        let last = path[path.len() - 2];
                        facing = wow_facing(end[0] - last[0], end[1] - last[1]);
                        self.spline_id = self.spline_id.wrapping_add(1);
                        self.moves += 1;
                        self.emit(SessionEvent::MonsterMove {
                            guid,
                            transport: None,
                            start: from,
                            spline_id: self.spline_id,
                            // A path starts at the wire's start point, as the packet decodes it:
                            // a one-point path installs no spline (`monster_move_spline`).
                            path,
                            facing: MonsterMoveFacing::None,
                            stop: false,
                            duration_ms: (steps as f32 * TICK_MS).round() as u32,
                            flying: false,
                            run_mode: true,
                        });
                        shown_at = end;
                        until = self.frames + steps as u64;
                    } else if let Some(t) = target {
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
                }
            } else {
                let guid = self.next_creature;
                self.next_creature += 1;
                let player = actor["kind"].as_str() == Some("player");
                let key = actor["model"].as_str().unwrap_or("Kobold").to_owned();
                let display = self.models.resolve(&key);
                let facing = target
                    .map(|t| (t[0] - pos[0], t[1] - pos[1]))
                    .filter(|(tx, ty)| tx.hypot(*ty) > 0.05)
                    .map_or(0.0, |(tx, ty)| wow_facing(tx, ty));
                self.emit(unit_create(
                    COMBAT_WALK_SPEED,
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
                self.shown.insert(
                    id.to_owned(),
                    Shown { guid, pos, life, facing, walk_until: 0, last_dqq: pos },
                );
            }
        }
        let gone: Vec<String> = self.shown.keys().filter(|k| !seen.contains(k)).cloned().collect();
        for key in gone {
            if let Some(s) = self.shown.remove(&key) {
                self.emit(SessionEvent::ObjectDestroyed(s.guid));
            }
        }
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

/// A unit or player create, as the update stream carries it: `OBJECT_FIELD_TYPE`, the display
/// ids and the given fields.
fn unit_create(
    walk: f32,
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
    ];
    all.extend_from_slice(pairs);
    SessionEvent::ObjectCreate {
        guid,
        kind,
        display_id: Some(display),
        position,
        orientation,
        scale: 1.0,
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
