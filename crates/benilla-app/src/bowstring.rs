//! The engine-drawn bowstring: bow M2s have no string geometry. The reference's per-frame callback
//! (`0x611ff0`) draws a two-segment line between the `$WTT`/`$WTB` limb-tip markers, its middle
//! vertex at the HandArrow attach while the nock latch (`[+0xd58] & 0x4000`) holds, else the tip
//! midpoint. Drawn here as gizmo lines; the color and width are inferred, since the reference's
//! packed vertex color is not decoded.
//!
//! The tips are posed: `$BWP` plays BowPull (160) on the prop and `$BWR` returns it to Stand (0),
//! so the markers ride limb bones that bend through the draw.

use bevy::prelude::*;

use crate::creature_anim::NockLatch;
use crate::entities::BoneAttach;

/// The longest a drawn string may span, yards at scale 1 (times the prop's scale): a bow's tips
/// (under two yards apart) and the draw to the hand stay inside it.
const MAX_SPAN: f32 = 2.5;

/// The HandArrow attach id (35, `0x6121b8`): the string's middle point while nocked.
const HAND_ARROW: u16 = 0x23;

/// Marks a bow prop root whose model authors the `$WTT`/`$WTB` anchors; despawned with the prop.
#[derive(Component)]
pub(crate) struct Bowstring {
    /// The unit wearing the bow, which carries the HandArrow attach and the nock latch.
    pub(crate) owner: Entity,
    /// The `$WTT` top and `$WTB` bottom anchors as `(bone, model-local offset)`.
    pub(crate) top: (u16, Vec3),
    pub(crate) bottom: (u16, Vec3),
}

/// Draws every visible bow's string, tip to middle to tip, from this frame's joint frames.
fn draw_bowstrings(
    bows: Query<(
        &Bowstring,
        &GlobalTransform,
        &InheritedVisibility,
        Option<&benilla_world::rig_anim::RigPose>,
    )>,
    owners: Query<(
        &BoneAttach,
        &benilla_world::rig_anim::RigPose,
        Has<NockLatch>,
    )>,
    joints: Query<&GlobalTransform>,
    mut gizmos: Gizmos,
) {
    // Inferred: the reference's packed vertex color is not decoded.
    const STRING_COLOR: Color = Color::srgb(0.12, 0.10, 0.08);
    for (bs, prop, vis, flex) in &bows {
        if !vis.get() {
            continue;
        }
        // The prop's own pose when it flexes (`joints_root` is this entity); its rigid frame
        // otherwise.
        let tip = |(bone, offset): (u16, Vec3)| {
            flex.and_then(|p| p.posed_point(prop, bone, offset))
                .unwrap_or_else(|| prop.transform_point(offset))
        };
        // Exile deviation (user 2026-11-09: a string ran far past the bow): a bow is under two
        // yards tip to tip, so a pose that puts its tips further apart (a bone frame from another
        // rig, a prop not yet placed) falls back to the rigid frame, and a string still that
        // long is not drawn; the nocked middle stays within reach of the tips.
        let span = MAX_SPAN * prop.compute_transform().scale.max_element().max(1.0);
        let sane = |a: Vec3, b: Vec3| a.is_finite() && b.is_finite() && a.distance(b) < span;
        let (mut top, mut bottom) = (tip(bs.top), tip(bs.bottom));
        if !sane(top, bottom) {
            top = prop.transform_point(bs.top.1);
            bottom = prop.transform_point(bs.bottom.1);
            if !sane(top, bottom) {
                continue;
            }
        }
        let mid = (top + bottom) / 2.0;
        let middle = owners
            .get(bs.owner)
            .ok()
            .filter(|(_, _, latched)| *latched)
            .and_then(|(bones, pose, _)| {
                let &(bone, offset) = bones.points.get(&HAND_ARROW)?;
                pose.posed_point(joints.get(pose.joints_root).ok()?, bone, offset)
            })
            .filter(|m| sane(*m, mid))
            .unwrap_or(mid);
        gizmos.line(top, middle, STRING_COLOR);
        gizmos.line(middle, bottom, STRING_COLOR);
    }
}

/// Registers the string drawer after the palette pass, beside the other joint-frame readers.
pub(crate) struct BowstringPlugin;

impl Plugin for BowstringPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(
            PostUpdate,
            draw_bowstrings.in_set(benilla_world::billboard::BillboardPlace),
        );
    }
}
