//! Dest-anchored spell effects: what a ground cast shows at its point. A DynamicObject anchors a
//! persistent area effect with two visuals, its own looping model and a shard emitter, outside the
//! unit kit pipeline (no `PlaySpellVisualKit` `0x60edf0` call site is on this class), and
//! `SMSG_SPELL_GO` plays a one-shot at the dest point.

use std::collections::HashMap;

use bevy::prelude::*;

use crate::creature_anim::{SpellKitSound, SpellVisuals};
use crate::net::{NetEntity, ObjectStore};
use benilla_protocol::EntityKind;

use super::spell_fx::{attach_effect_visuals, ensure_model, FxMaterials, SpellFx};

/// The client's hardcoded shard-model table (`0x870e24`), indexed by `CharParamZero`'s small int.
const SHARD_MODELS: [&str; 7] = [
    "Spells\\Blizzard_Impact_Base.mdx",
    "Spells\\RainOfFire_Impact_Base.mdx",
    "Spells\\CallLightning_Impact.mdx",
    "Spells\\FlamestrikeSmall_Impact_Base.mdx",
    "Spells\\DeathAndDecay_Area_Base.mdx",
    "Spells\\ArcaneShot_Area.mdx",
    "Spells\\StarShards_Impact_Base.mdx",
];

/// The `CharProcType` whose first kit block the emitter chain takes (`0x5d55c0`).
const PROC_TYPE_SHARD_EMITTER: i32 = 9;

/// `CharParamZero`'s small int ([`benilla_formats::char_proc_small_int`]), clamped into
/// [`SHARD_MODELS`]: the client has no bounds check (`mov cl,al` in `0x5d55c0`) and reads past it.
fn shard_model_index(param0: f32) -> usize {
    let idx = benilla_formats::char_proc_small_int(param0) as usize;
    idx.min(SHARD_MODELS.len() - 1)
}

/// The shard emitter's rate factor from the `spellEffectLevel` record's integer, read once at the
/// emitter's Initialize (`0x6eb95e`-`0x6eb989`), so a change reaches only emitters created after
/// it: 0.33 at 0, 0.66 at 1, and 1.0 for any other value, unclamped, or with no record.
fn shard_rate_scale(spell_effect_level: Option<i32>) -> f32 {
    spell_effect_level.map_or(1.0, crate::video::spell_effect_scale)
}

/// A dest-anchored effect model: visual A, a shard or a GO burst.
#[derive(Component)]
pub(super) struct GroundFx {
    /// The [`SpellFx`] model-cache key.
    path: String,
    /// Loop for life (visual A), or one pass of sequence 0 then despawn (a shard, a burst).
    looping: bool,
    /// Parts attached (the model was ready).
    spawned: bool,
    loop_armed: bool,
    /// One-shot self-termination deadline (`time.elapsed_secs()`), set at attach.
    expires: Option<f32>,
    /// An Exile range circle (its lifecycle is held, not repeated); the id is unused.
    anim: Option<u16>,
    /// The stretch of its opening a range circle keeps playing, seconds: from the first to the
    /// second, then back; equal, it stands still there.
    window: Option<(f32, f32)>,
}

impl GroundFx {
    fn new(path: String, looping: bool) -> Self {
        Self {
            path,
            looping,
            spawned: false,
            loop_armed: false,
            expires: None,
            anim: None,
            window: None,
        }
    }
}

/// Visual B, a DynamicObject's shard emitter (`AUBlizzardObject`, `0x5d55c0`, `0x6ece30`). It dies
/// with the anchor (`0x6ecf20` zeroes the rate); its shards are free entities that run out their
/// own lifetimes.
#[derive(Component)]
pub(super) struct ShardEmitter {
    /// The [`SHARD_MODELS`] path.
    path: String,
    /// `DYNAMICOBJECT_RADIUS`: the wire radius is the spread (`0x6ebad0`).
    radius: f32,
    /// Shards per second: `CharParamOne` times the `spellEffectLevel` factor (`0x6eb95e`-
    /// `0x6eb98f`), 1.0 at its default "2".
    rate: f32,
    /// Fractional emissions carried between frames.
    accum: f32,
    /// xorshift* state for the spawn offsets, seeded per emitter: no rand dependency.
    rng: u64,
}

/// The router's dest one-shot: `SMSG_SPELL_GO` to a dest location plays `SpellVisual` field 12 once
/// at the point when field 6, the missile, is 0 (`0x6e8088`..`0x6e8143`).
#[derive(Message, Clone)]
pub(crate) struct GroundBurst {
    /// The field-12 `SpellVisualEffectName` model path.
    pub(crate) path: String,
    /// The dest point, in Bevy coordinates.
    pub(crate) pos: Vec3,
}

/// Arm each new DynamicObject off its create's fields: visual A, the shard emitter and the area
/// sound. A re-cast is a new guid, never a re-create in place.
pub(super) fn arm_ground_effects(
    mut commands: Commands,
    created: Query<(Entity, &NetEntity, &ObjectStore), Added<ObjectStore>>,
    visuals: Option<Res<SpellVisuals>>,
    spells: Option<Res<crate::ui_action::Spells>>,
    fx: Option<ResMut<SpellFx>>,
    asset_server: Res<AssetServer>,
    mut sounds: MessageWriter<SpellKitSound>,
    spell_effect_level: Option<Res<crate::video::SpellEffectLevel>>,
) {
    let (Some(visuals), Some(spells), Some(mut fx)) = (visuals, spells, fx) else {
        return;
    };
    let level_scale = shard_rate_scale(spell_effect_level.map(|l| l.0));
    for (anchor, net, store) in &created {
        if net.kind != EntityKind::DynamicObject {
            continue;
        }
        let Some(spell_id) = store.0.dynamicobject_spell_id() else {
            continue;
        };
        let Some(stages) = spells
            .catalog
            .get(spell_id)
            .and_then(|d| visuals.0.stages(d.visual))
        else {
            debug!("dest_fx: dynobj spell {spell_id} has no visual row — invisible area");
            continue;
        };
        let facing = store
            .0
            .dynamicobject_position()
            .map(|(_, f)| f)
            .unwrap_or(0.0);
        // Visual A (`0x5d57c0`): field 12's model when field 11 is set, at the object's position
        // with no terrain projection, turned by `DYNAMICOBJECT_FACING` (`0x613ef0`, `0x7bdd60`);
        // as a child it goes with the anchor.
        if stages.area_gate != 0 && stages.area_effect != 0 {
            if let Some(path) = visuals.0.effect_path(stages.area_effect) {
                let path = path.to_string();
                ensure_model(&mut fx, &asset_server, &path);
                let child = commands
                    .spawn((
                        GroundFx::new(path, true),
                        Transform::from_rotation(Quat::from_rotation_y(facing)),
                        Visibility::default(),
                    ))
                    .id();
                commands.entity(anchor).add_child(child);
            }
        }
        // Visual B from field 13's kit, whose sound loops as the area sound: stopped at destroy,
        // where the reference fades it over 3 s. A kit with no type-9 block sounds here; whether
        // the reference, which ties the sound to the emitter, sounds it is untraced.
        if let Some(kit) = visuals.0.kit(stages.area_kit) {
            if let Some(proc) = kit.char_procs().find(|p| p.ty == PROC_TYPE_SHARD_EMITTER) {
                let path = SHARD_MODELS[shard_model_index(proc.params[0])].to_string();
                ensure_model(&mut fx, &asset_server, &path);
                commands.entity(anchor).insert(ShardEmitter {
                    path,
                    radius: store.0.dynamicobject_radius().unwrap_or(0.0),
                    rate: proc.params[1] * level_scale,
                    accum: 0.0,
                    rng: 0x9e3779b97f4a7c15 ^ anchor.to_bits(),
                });
            }
            if let Some(kit_sound) = kit.sound {
                sounds.write(SpellKitSound::Play {
                    entity: anchor,
                    spell_id,
                    kit_sound,
                    cast_hold: false,
                    // A ground arrival of the frame, after every packet.
                    at: None,
                });
            }
        }
        debug!(
            "dest_fx: dynobj armed — spell {spell_id}, gateA={} effect={} kit={} radius {:?}",
            stages.area_gate,
            stages.area_effect,
            stages.area_kit,
            store.0.dynamicobject_radius(),
        );
    }
}

/// Exile of Azeroth: a range circle under a unit ([`crate::net::set_ground_ring`]): the area
/// effect model of a borrowed ground spell (Consecration's circle…), replayed pass after pass,
/// scaled to the range and kept under the unit as it moves. A deviation: the reference draws these only for a
/// DynamicObject, unscaled.
#[derive(Component)]
pub(super) struct ExileRing {
    guid: u64,
    ring: (u32, f32),
}

/// Where a held range circle stops, seconds into its opening: drawn in full there, faded by its
/// end (the aura rings' `Stand` runs about 1.4 s).
const RING_DRAWN_AT: f32 = 0.8;

/// The stretch of an area model's opening a range circle keeps playing, as fractions of it: past
/// the spread, before the fade (a burning ground is lava from about a quarter to two thirds in).
const AREA_FULL_FROM: f32 = 0.3;
const AREA_FULL_TO: f32 = 0.6;

/// How long a range circle's next pass overlaps the one ending, seconds.
const RING_OVERLAP: f32 = 0.8;

/// Spawn, follow and remove the range circles.
pub(super) fn exile_ground_rings(
    mut commands: Commands,
    mut rings: Query<(Entity, &ExileRing, &mut Transform, &GroundFx)>,
    time: Res<Time>,
    index: Option<Res<crate::net::GuidIndex>>,
    units: Query<&GlobalTransform, Without<ExileRing>>,
    visuals: Option<Res<SpellVisuals>>,
    spells: Option<Res<crate::ui_action::Spells>>,
    fx: Option<ResMut<SpellFx>>,
    asset_server: Res<AssetServer>,
) {
    if !crate::net::exile_mode() {
        return;
    }
    let (Some(index), Some(visuals), Some(spells), Some(mut fx)) = (index, visuals, spells, fx) else {
        return;
    };
    let mut wanted = crate::net::ground_rings();
    if wanted.is_empty() && rings.is_empty() {
        return;
    }
    let under = |guid: u64| index.0.get(&guid).and_then(|e| units.get(*e).ok()).map(GlobalTransform::translation);
    let now = time.elapsed_secs();
    // Each circle's newest pass: the next starts [`RING_OVERLAP`] before it ends, so the passes
    // cross-fade instead of leaving a dim gap.
    let mut newest: HashMap<u64, Option<f32>> = HashMap::new();
    let wanted_now = wanted.clone();
    for (entity, ring, mut tf, gfx) in &mut rings {
        match (wanted_now.get(&ring.guid), under(ring.guid)) {
            (Some(now_ring), Some(at)) if *now_ring == ring.ring => {
                if tf.translation != at {
                    tf.translation = at;
                }
                // A pass not yet built (no expiry) counts as fresh.
                let end = if gfx.spawned { gfx.expires } else { None };
                let e = newest.entry(ring.guid).or_insert(end);
                *e = match (*e, end) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    _ => None,
                };
            }
            _ => {
                // Gone, or changed: a changed circle is drawn anew next frame.
                commands.entity(entity).despawn();
            }
        }
    }
    wanted.retain(|guid, _| match newest.get(guid) {
        None => true,
        Some(None) => false,
        Some(Some(end)) => now >= end - RING_OVERLAP,
    });
    for (guid, (spell_id, scale)) in wanted {
        let Some(at) = under(guid) else { continue };
        // The spell's ground model: its area model (a DynamicObject's: Consecration, Hellfire),
        // else its cast kit's model (an aura's ring at the feet: Retribution Aura).
        let stages = spells.catalog.get(spell_id).and_then(|d| visuals.0.stages(d.visual));
        let path = stages
            .filter(|s| s.area_effect != 0)
            .and_then(|s| visuals.0.effect_path(s.area_effect))
            .or_else(|| {
                let kit = visuals.0.kit(stages?.cast)?;
                let effect = kit.effects().map(|(_, e)| e).next()?;
                visuals.0.effect_path(effect)
            });
        let Some(path) = path else {
            warn!("exile: range circle spell {spell_id} has no area effect model");
            crate::net::set_ground_ring(guid, None);
            continue;
        };
        let path = path.to_string();
        ensure_model(&mut fx, &asset_server, &path);
        // One instance for as long as the circle is wanted (user 2026-11-11: lit while the aura
        // lasts, not replayed): its opening plays once, then the stretch where it is drawn in full
        // runs over and over (a burning ground's few seconds of lava), or, for an aura ring
        // (which authors `Hold`, drawing nothing here), its drawn frame stands still.
        let Some(model) = fx.models.get(&path) else { continue };
        if model.parts.is_none() {
            continue; // still loading: decided once it is in
        }
        let Some(anims) = model.animations.as_ref() else { continue };
        let ring_model = anims.find(super::spell_fx::ANIM_HOLD).is_some();
        let duration = anims.preferred_clip(None).map_or(0.0, |c| c.duration);
        let window = if ring_model {
            (RING_DRAWN_AT, RING_DRAWN_AT)
        } else {
            (duration * AREA_FULL_FROM, duration * AREA_FULL_TO)
        };
        info!(
            "exile: range circle {path} under {guid:#x}, scale {scale:.2}, held {:.2}..{:.2} s of {duration:.2}",
            window.0, window.1
        );
        let mut gfx = GroundFx::new(path, true);
        gfx.anim = Some(0);
        gfx.window = Some(window);
        commands.spawn((
            gfx,
            ExileRing { guid, ring: (spell_id, scale) },
            Transform::from_translation(at).with_scale(Vec3::splat(scale)),
            Visibility::default(),
        ));
    }
}

/// Spawn the GO dest one-shots at once, never waiting on the DynamicObject create that follows.
pub(super) fn spawn_ground_bursts(
    mut commands: Commands,
    mut bursts: MessageReader<GroundBurst>,
    fx: Option<ResMut<SpellFx>>,
    asset_server: Res<AssetServer>,
) {
    let Some(mut fx) = fx else { return };
    for burst in bursts.read() {
        ensure_model(&mut fx, &asset_server, &burst.path);
        commands.spawn((
            GroundFx::new(burst.path.clone(), false),
            Transform::from_translation(burst.pos),
            Visibility::default(),
        ));
    }
}

/// Emit `rate` shards per second, each a free one-shot at a uniform random point of the radius's
/// horizontal disc (the reference's spread scaled by `+0x11c`).
pub(super) fn tick_shard_emitters(
    mut commands: Commands,
    time: Res<Time>,
    mut emitters: Query<(&mut ShardEmitter, &GlobalTransform)>,
) {
    for (mut em, tf) in &mut emitters {
        em.accum += em.rate * time.delta_secs();
        while em.accum >= 1.0 {
            em.accum -= 1.0;
            let mut next = || {
                em.rng ^= em.rng >> 12;
                em.rng ^= em.rng << 25;
                em.rng ^= em.rng >> 27;
                em.rng.wrapping_mul(0x2545F4914F6CDD1D)
            };
            let u1 = (next() >> 40) as f32 / (1u64 << 24) as f32;
            let u2 = (next() >> 40) as f32 / (1u64 << 24) as f32;
            let r = em.radius * u1.sqrt();
            let theta = u2 * std::f32::consts::TAU;
            let offset = Vec3::new(r * theta.cos(), 0.0, r * theta.sin());
            commands.spawn((
                GroundFx::new(em.path.clone(), false),
                Transform::from_translation(tf.translation() + offset),
                Visibility::default(),
            ));
        }
    }
}

/// Attach each pending instance's parts once its M2 builds, start the one-shot clocks, arm the
/// loops and despawn expired one-shots.
pub(super) fn attach_ground_fx_models(
    mut commands: Commands,
    time: Res<Time>,
    mut instances: Query<(Entity, &mut GroundFx, Option<&mut AnimationPlayer>)>,
    fx: Option<ResMut<SpellFx>>,
    asset_server: Res<AssetServer>,
    mut wow_materials: ResMut<Assets<benilla_assets::materials::WowModelMaterial>>,
    mut tint_reg: ResMut<super::spell_fx::FxTintAnims>,
    mut uv_reg: ResMut<benilla_world::doodad_anim::UvAnimMaterials>,
    mut anim_table: ResMut<benilla_world::mat_anim_table::MatAnimTable>,
    ibps: Res<Assets<bevy::mesh::skinning::SkinnedMeshInverseBindposes>>,
    mut palettes: ResMut<benilla_world::rig_palette::RigPalettes>,
) {
    let Some(mut fx) = fx else { return };
    let now = time.elapsed_secs();
    for (entity, mut inst, mut player) in &mut instances {
        if !inst.spawned {
            ensure_model(&mut fx, &asset_server, &inst.path);
            let Some(dm) = fx.models.get(&inst.path) else {
                continue; // unreachable: just inserted
            };
            if !attach_effect_visuals(
                &mut commands,
                entity,
                dm,
                now,
                true, // a dest-anchored model's flat quads are ground decals
                // Chained to nothing: its trail stays world-frozen and its pool finishes in place.
                super::spell_fx::EffectHost::default(),
                // Not a `CEffect` on a unit: its own span clock, the plain single-clip arm. An Exile
                // range circle plays its opening (`Stand`) once and holds its last frame, the
                // drawn circle, for as long as it is wanted: the ring models' `Hold` draws nothing
                // here. Riding its player's clock, its fades hold with it.
                inst.anim.map(|_| crate::creature_anim::FxStage::OneShot),
                &mut FxMaterials {
                    store: &mut wow_materials,
                    tint: &mut tint_reg,
                    uv: &mut uv_reg,
                    table: &mut anim_table,
                },
                &ibps,
                &mut palettes,
                None,
            ) {
                continue; // model still building
            }
            inst.spawned = true;
            if !inst.looping {
                // One pass of the first sequence, as `spell_fx`'s span clock times a kit.
                let span = dm.first_seq_span.unwrap_or(super::spell_fx::FALLBACK_SPAN);
                inst.expires = Some(now + span);
            }
            continue; // the AnimationPlayer lands next frame
        }
        // The reference re-fires on completion (`0x5d5580`), the hardcoded sequence `0x9e` when
        // `obj+0x190` bit 1 is set (its source untraced); this repeats the first clip.
        // (A range circle's lifecycle hands its birth over to `Hold` itself: no repeat over it.)
        // An Exile range circle stops at its drawn frame, [`RING_DRAWN_AT`] into its opening,
        // and stays so: its fades ride the player's clock, so they hold too (user 2026-11-11:
        // the circle stays lit while the aura lasts, no replay).
        if let Some((from, to)) = inst.window {
            if let Some(player) = player.as_mut() {
                for (_, anim) in player.playing_animations_mut() {
                    if anim.seek_time() >= to {
                        if to - from < 0.05 {
                            anim.set_speed(0.0);
                        } else {
                            anim.seek_to(from);
                        }
                    }
                }
            }
        }
        if inst.looping && inst.anim.is_none() && !inst.loop_armed {
            if let Some(player) = player.as_mut() {
                // Armed only once a clip is playing: the player can land a frame before its clip
                // starts, and arming then repeated nothing, so the loop played once and faded
                // (Exile's range circles, user 2026-11-11).
                let mut armed = false;
                for (_, anim) in player.playing_animations_mut() {
                    anim.repeat();
                    armed = true;
                }
                inst.loop_armed = armed;
            }
        }
        if let Some(expires) = inst.expires {
            if now >= expires {
                commands.entity(entity).despawn();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shard_rate_follows_the_spell_effect_level_record() {
        assert_eq!(shard_rate_scale(Some(0)), 0.33);
        assert_eq!(shard_rate_scale(Some(1)), 0.66);
        assert_eq!(shard_rate_scale(Some(2)), 1.0);
        // The record's integer, not the handler's clamped copy: out of range is the ×1.0 arm.
        assert_eq!(shard_rate_scale(Some(-1)), 1.0);
        assert_eq!(shard_rate_scale(Some(5)), 1.0);
        assert_eq!(shard_rate_scale(None), 1.0);
    }

    /// The `0x5d55c0` decode, `bits(f32(param0 + 512.0)) >> 14 & 0xff`; the real rows carry 0.0
    /// (Blizzard) and 1.0 (Rain of Fire).
    #[test]
    fn shard_model_index_decodes_and_clamps() {
        assert_eq!(shard_model_index(0.0), 0);
        assert_eq!(shard_model_index(1.0), 1);
        assert_eq!(shard_model_index(6.0), 6);
        assert_eq!(shard_model_index(7.0), 6, "clamped, not read past");
        assert_eq!(shard_model_index(200.0), 6);
        assert_eq!(SHARD_MODELS[0], "Spells\\Blizzard_Impact_Base.mdx");
        assert_eq!(SHARD_MODELS[1], "Spells\\RainOfFire_Impact_Base.mdx");
    }
}
