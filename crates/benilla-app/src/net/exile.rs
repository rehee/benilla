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
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use benilla_protocol::field::{
    FIELD_UNIT_DISPLAYID, FIELD_UNIT_FACTIONTEMPLATE, FIELD_UNIT_HEALTH, FIELD_UNIT_LEVEL,
    FIELD_UNIT_MAXHEALTH,
};
use benilla_protocol::messages::{AttackerState, Billing, ObjectType};
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

/// The battlefield anchor: Northshire, the client's own boot anchor, map 0 (Eastern Kingdoms).
/// One DQQ unit is [`YARDS_PER_UNIT`] yards (architecture §5.2).
const MAP_ID: u32 = 0;
const ORIGIN: [f32; 3] = [-8949.95, -132.49, 83.6];
const YARDS_PER_UNIT: f32 = 3.0;

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
            Err(tungstenite::Error::Io(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
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
}

/// How long after entering the world the auto combat starts: the entry load must have drained,
/// or the frames land on a loading screen.
const AUTOCOMBAT_DELAY: Duration = Duration::from_secs(10);
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
    /// DQQ actor id → its world unit.
    shown: HashMap<String, Shown>,
    next_creature: u64,
    /// The `combatId` being shown.
    combat: Option<String>,
    models: ModelBook,
    /// When the auto combat is due, and when a finished combat's units go.
    combat_due: Option<Instant>,
    clear_due: Option<Instant>,
    spline_id: u32,
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
            shown: HashMap::new(),
            next_creature: 0xF130_0000_0000_0001,
            combat: None,
            models: ModelBook::load(),
            combat_due: None,
            clear_due: None,
            spline_id: 0,
        }
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
                recv(cmd_rx) -> m => if m.is_err() { return },
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

    /// The delayed auto combat and the clear after a combat.
    fn timers(&mut self) {
        let now = Instant::now();
        if self.combat_due.is_some_and(|t| now >= t) {
            self.combat_due = None;
            let id = self.self_guid.and_then(|g| self.dqq_ids.get(&g).cloned());
            self.send("combat.start", json!({ "characterId": id, "map": "Map_1_1", "mode": "watch" }));
        }
        if self.clear_due.is_some_and(|t| now >= t) {
            self.clear_due = None;
            let guids: Vec<u64> = self.shown.drain().map(|(_, s)| s.guid).collect();
            for guid in guids {
                self.emit(SessionEvent::ObjectDestroyed(guid));
            }
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
                bevy::log::info!("exile: combat begins {data}");
            }
            "combat.frame" => self.frame(data),
            "combat.end" => {
                bevy::log::info!("exile: combat ends {data}");
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
    /// player's own unit. `EXILE_AUTOCOMBAT=1` then asks the runtime for a watched combat.
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
        let bytes0 = me.race as u32 | (me.class as u32) << 8 | (me.gender as u32) << 16;
        self.emit(unit_create(
            guid,
            EntityKind::Player,
            50,
            ORIGIN,
            0.0,
            &[
                (FIELD_UNIT_HEALTH, 100),
                (FIELD_UNIT_MAXHEALTH, 100),
                (FIELD_UNIT_LEVEL, me.level as u32),
                (FIELD_UNIT_FACTIONTEMPLATE, FACTION_PLAYER),
                (FIELD_UNIT_BYTES_0, bytes0),
            ],
        ));
        if std::env::var("EXILE_AUTOCOMBAT").as_deref() == Ok("1") {
            self.combat_due = Some(Instant::now() + AUTOCOMBAT_DELAY);
        }
    }

    /// One `combat.frame`: create the actors not yet shown, walk the rest to their new spot
    /// (`MonsterMove`, so the client animates the walk), update health, remove those gone, then play
    /// the frame's damage as melee swings with floating numbers (`AttackerState`). The hero is a
    /// unit of its own, apart from the (hidden) player (§5.1).
    fn frame(&mut self, data: &Value) {
        let mut seen = Vec::new();
        for actor in data["actors"].as_array().into_iter().flatten() {
            let Some(id) = actor["id"].as_str() else { continue };
            seen.push(id.to_owned());
            let pos = to_world(actor["x"].as_f64().unwrap_or(0.0), actor["y"].as_f64().unwrap_or(0.0));
            let facing = actor["angle"].as_f64().unwrap_or(0.0) as f32;
            let max_life = actor["maxLife"].as_f64().unwrap_or(100.0).max(1.0).ceil() as u32;
            let life = actor["life"].as_f64().unwrap_or(max_life as f64).max(0.0).ceil() as u32;
            if let Some(shown) = self.shown.get(id) {
                let (guid, from, old_life) = (shown.guid, shown.pos, shown.life);
                let moved = (pos[0] - from[0]).hypot(pos[1] - from[1]);
                if moved > 0.05 {
                    self.spline_id = self.spline_id.wrapping_add(1);
                    self.emit(SessionEvent::MonsterMove {
                        guid,
                        transport: None,
                        start: from,
                        spline_id: self.spline_id,
                        path: vec![pos],
                        facing: MonsterMoveFacing::None,
                        stop: false,
                        duration_ms: 40,
                        flying: false,
                        run_mode: true,
                    });
                }
                if life != old_life {
                    self.emit(SessionEvent::ObjectValues {
                        guid,
                        fields: ObjectFields::from_pairs(&[(FIELD_UNIT_HEALTH, life)]),
                    });
                }
                if let Some(s) = self.shown.get_mut(id) {
                    s.pos = pos;
                    s.life = life;
                }
            } else {
                let guid = self.next_creature;
                self.next_creature += 1;
                let player = actor["kind"].as_str() == Some("player");
                let key = actor["model"].as_str().unwrap_or("Kobold").to_owned();
                let display = self.models.resolve(&key);
                self.emit(unit_create(
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
                self.shown.insert(id.to_owned(), Shown { guid, pos, life });
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

/// DQQ battlefield `(x, y)` (double) → WoW world coordinates around [`ORIGIN`]; the ground height
/// is the client's ground clamp's to settle.
fn to_world(x: f64, y: f64) -> [f32; 3] {
    [
        ORIGIN[0] + x as f32 * YARDS_PER_UNIT,
        ORIGIN[1] + y as f32 * YARDS_PER_UNIT,
        ORIGIN[2],
    ]
}

/// A unit or player create, as the update stream carries it: `OBJECT_FIELD_TYPE`, the display
/// ids and the given fields.
fn unit_create(
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
            walk: 2.5,
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
