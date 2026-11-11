//! The Exile transmog window's item lists (PLAN-0011): every look `ItemDisplayInfo.dbc` holds,
//! sorted into the WoW slot it is worn in by its model and texture names (the 1.12 client has no
//! item table: names and slots live on the server). Each entry carries the transmog value the
//! runtime's character config takes (`equipment.json`'s item form: display, type, weapon, slot),
//! so a bow lands in the main hand as a bow (user 2026-11-11: bows and wands are main hand) and a
//! one-hand weapon is offered for either hand. The runtime's named items (`wow-items.json`,
//! Thunderfury…) come first in their slots.

use std::collections::{HashMap, HashSet};

use benilla_formats::{ItemDisplay, ItemDisplayCatalog};
use serde_json::{json, Value};

/// The window's slots, in its order (the runtime's slot names).
pub(super) const SLOTS: [&str; 13] = [
    "head", "shoulder", "back", "chest", "shirt", "tabard", "wrist", "hands", "waist", "legs", "feet",
    "mainHand", "offHand",
];

/// Rows per page in the window.
pub(super) const PAGE: usize = 10;

/// One look the window offers.
#[derive(Debug, Clone)]
pub(super) struct Look {
    /// `d<display>` for a catalogue look, `n<name>` for a named item.
    pub key: String,
    pub icon: String,
    pub name: String,
    /// The transmog value sent to the runtime.
    pub value: Value,
}

pub(super) struct Wardrobe {
    by_slot: HashMap<&'static str, Vec<Look>>,
    named: HashMap<&'static str, Vec<Look>>,
}

const NO_ICON: &str = "Interface\\Icons\\INV_Misc_QuestionMark";

/// Model name prefix → (kind, weapon). Kind: one (either hand), two, ranged, gun, wand, thrown,
/// shield, holdable, head, shoulder.
const MODELS: &[(&str, &str, Option<&str>)] = &[
    ("sword_1h", "one", Some("sword")),
    ("sword_2h", "two", Some("sword2h")),
    ("axe_1h", "one", Some("axe")),
    ("axe_2h", "two", Some("axe2h")),
    ("mace_1h", "one", Some("mace")),
    ("hammer_1h", "one", Some("mace")),
    ("club_1h", "one", Some("mace")),
    ("mace_2h", "two", Some("mace2h")),
    ("hammer_2h", "two", Some("mace2h")),
    ("totem_2h", "two", Some("mace2h")),
    ("stave_", "two", Some("staff")),
    ("staff_", "two", Some("staff")),
    ("polearm_", "two", Some("polearm")),
    ("knife_", "one", Some("dagger")),
    ("dagger", "one", Some("dagger")),
    ("glave_1h", "one", Some("sword")),
    ("fist_", "one", Some("fist")),
    ("claw_", "one", Some("fist")),
    ("hand_1h", "one", Some("fist")),
    ("crossbow_", "ranged", Some("crossbow")),
    ("bow_", "ranged", Some("bow")),
    ("firearm_", "gun", Some("gun")),
    ("gun_", "gun", Some("gun")),
    ("wand_", "wand", Some("wand")),
    ("thrown_", "thrown", Some("thrown")),
    ("shield_", "shield", None),
    ("buckler_", "shield", None),
    ("offhand_", "holdable", None),
    ("misc_", "holdable", None),
    ("helm_", "head", None),
    ("lshoulder_", "shoulder", None),
    ("shoulder_", "shoulder", None),
];

/// Icon name prefix → slot (worn armour, checked before the texture names).
const ICONS: &[(&str, &str)] = &[
    ("inv_chest", "chest"),
    ("inv_boots", "feet"),
    ("inv_pants", "legs"),
    ("inv_belt", "waist"),
    ("inv_bracer", "wrist"),
    ("inv_gauntlets", "hands"),
    ("inv_misc_cape", "back"),
    ("inv_shirt", "shirt"),
    ("inv_banner", "tabard"),
];

/// Body texture name part → slot, in priority order.
const TEXTURES: &[(&str, &str)] = &[
    ("tabard", "tabard"),
    ("shirt", "shirt"),
    ("_chest_", "chest"),
    ("_sleeve_", "chest"),
    ("_pant", "legs"),
    ("_belt_", "waist"),
    ("_boot_", "feet"),
    ("_glove_", "hands"),
    ("_bracer_", "wrist"),
    ("_robe", "legs"),
];

fn base(name: &str) -> String {
    let file = name.rsplit(['\\', '/']).next().unwrap_or(name);
    file.split('.').next().unwrap_or(file).to_owned()
}

/// A look's slots and transmog value; `None` for what is not worn gear (quivers, NPC props).
fn classify(display: u32, d: &ItemDisplay) -> Option<(Vec<&'static str>, Value, String)> {
    let model = d.model[0].as_deref().map(base).unwrap_or_default().to_lowercase();
    let texture = d.model_texture[0].clone().unwrap_or_default();
    let icon = d.icon.as_deref().map(base).unwrap_or_default().to_lowercase();
    if !model.is_empty() {
        let &(_, kind, weapon) = MODELS.iter().find(|(p, _, _)| model.starts_with(p))?;
        let name = if texture.is_empty() { base(d.model[0].as_deref().unwrap_or("")) } else { base(&texture) };
        let (slots, value) = match kind {
            "one" => (vec!["mainHand", "offHand"], json!({ "display": display, "type": "oneHand", "weapon": weapon })),
            "two" => (vec!["mainHand"], json!({ "display": display, "type": "twoHand", "weapon": weapon })),
            // Bows and guns sit in WoW's ranged slot (held in the left hand); the runtime adds the
            // main-hand twin for the stance (`MappingFiles.HeroDress`).
            "ranged" => (vec!["mainHand"], json!({ "display": display, "slot": "ranged", "type": "ranged", "weapon": weapon })),
            "gun" => (vec!["mainHand"], json!({ "display": display, "slot": "ranged", "type": "rangedRight", "weapon": weapon })),
            "wand" => (vec!["mainHand"], json!({ "display": display, "weapon": "wand" })),
            "thrown" => (vec!["mainHand"], json!({ "display": display, "slot": "ranged", "type": "thrown", "weapon": "thrown" })),
            "shield" => (vec!["offHand"], json!({ "display": display, "type": "shield" })),
            "holdable" => (vec!["offHand"], json!({ "display": display, "type": "holdable" })),
            "head" => (vec!["head"], json!({ "display": display })),
            _ => (vec!["shoulder"], json!({ "display": display })),
        };
        return Some((slots, value, name));
    }
    if texture.to_lowercase().contains("cape") || icon.starts_with("inv_misc_cape") {
        return Some((vec!["back"], json!({ "display": display }), if texture.is_empty() { icon } else { base(&texture) }));
    }
    let regions: Vec<&str> = d.region_textures.iter().flatten().map(String::as_str).collect();
    let joined = regions.join(" ").to_lowercase();
    let slot = ICONS
        .iter()
        .find(|(p, _)| icon.starts_with(p))
        .or_else(|| TEXTURES.iter().find(|(p, _)| joined.contains(p)))
        .map(|&(_, s)| s)?;
    // A robe: the chest's skirt geoset (`GeosetGroup2`), WoW's InventoryType 20.
    let value = if slot == "chest" && d.geoset_groups[2] == 1 {
        json!({ "display": display, "type": "robe" })
    } else {
        json!({ "display": display })
    };
    // Named by its first body texture, the region suffix (`_TU`, `_LL`…) cut.
    let first = base(regions.first().copied().unwrap_or(""));
    let name = match first.rsplit_once('_') {
        Some((stem, tail)) if tail.len() == 2 => stem.to_owned(),
        _ => first.clone(),
    };
    Some((vec![slot], value, name))
}

impl Wardrobe {
    pub(super) fn build(catalog: &ItemDisplayCatalog) -> Self {
        let mut ids: Vec<(u32, &ItemDisplay)> = catalog.iter().collect();
        ids.sort_by_key(|(id, _)| *id);
        let mut by_slot: HashMap<&'static str, Vec<Look>> = HashMap::new();
        let mut seen = HashSet::new();
        for (id, d) in ids {
            let Some((slots, value, name)) = classify(id, d) else { continue };
            // The same look under several displays (an item and its NPC copy): once.
            let same = (d.model.clone(), d.model_texture.clone(), d.region_textures.clone(), d.geoset_groups);
            if !seen.insert(same) || name.is_empty() {
                continue;
            }
            let icon = d.icon.clone().unwrap_or_else(|| NO_ICON.to_owned());
            for slot in slots {
                by_slot.entry(slot).or_default().push(Look {
                    key: format!("d{id}"),
                    icon: icon.clone(),
                    name: name.clone(),
                    value: value.clone(),
                });
            }
        }
        for list in by_slot.values_mut() {
            list.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        }
        bevy::log::info!(
            "exile: wardrobe {} look(s) in {} slot(s)",
            by_slot.values().map(Vec::len).sum::<usize>(),
            by_slot.len()
        );
        Self { by_slot, named: HashMap::new() }
    }

    /// The runtime's named items (`ui.wow.items.result`), by the slots their type fits.
    pub(super) fn set_named(&mut self, list: &Value, catalog: Option<&ItemDisplayCatalog>) {
        self.named.clear();
        for item in list.as_array().into_iter().flatten() {
            let Some(key) = item["key"].as_str() else { continue };
            let display = item["display"].as_u64().unwrap_or(0) as u32;
            let slots: Vec<&'static str> = match (item["type"].as_str(), item["weapon"].as_str()) {
                (Some("twoHand" | "ranged" | "rangedRight" | "thrown" | "mainHand"), _) | (_, Some("wand" | "bow" | "gun" | "crossbow")) => {
                    vec!["mainHand"]
                }
                (Some("shield" | "holdable" | "offHand"), _) => vec!["offHand"],
                (Some("oneHand"), _) | (None, Some(_)) => vec!["mainHand", "offHand"],
                (Some(t), _) => SLOTS.iter().copied().filter(|s| s.eq_ignore_ascii_case(t) || (t == "robe" && *s == "chest")).collect(),
                (None, None) => Vec::new(),
            };
            let icon = catalog
                .and_then(|c| c.get(display))
                .and_then(|d| d.icon.clone())
                .unwrap_or_else(|| NO_ICON.to_owned());
            for slot in slots {
                self.named.entry(slot).or_default().push(Look {
                    key: format!("n{key}"),
                    icon: icon.clone(),
                    name: format!("* {}", item["name"].as_str().unwrap_or(key)),
                    value: json!({ "item": key }),
                });
            }
        }
    }

    /// One page of a slot's list, narrowed by `search` (any part of the name): the rows and the
    /// page count.
    pub(super) fn page(&self, slot: &str, page: usize, search: &str) -> (Vec<&Look>, usize) {
        let needle = search.trim().to_lowercase();
        let all: Vec<&Look> = self
            .named
            .get(slot)
            .into_iter()
            .flatten()
            .chain(self.by_slot.get(slot).into_iter().flatten())
            .filter(|l| needle.is_empty() || l.name.to_lowercase().contains(&needle))
            .collect();
        let pages = all.len().div_ceil(PAGE).max(1);
        let page = page.min(pages - 1);
        (all.into_iter().skip(page * PAGE).take(PAGE).collect(), pages)
    }

    /// A look by its key in a slot's list.
    pub(super) fn find(&self, slot: &str, key: &str) -> Option<&Look> {
        self.named
            .get(slot)
            .into_iter()
            .flatten()
            .chain(self.by_slot.get(slot).into_iter().flatten())
            .find(|l| l.key == key)
    }
}
