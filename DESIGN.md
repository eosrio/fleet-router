---
name: fleet-router
description: Signal-box mimic panel for an Antelope SHiP reverse proxy; hairline seams, a 20px tile module, five lamp states.
colors:
  carbon: "#050505"
  field: "#0d0d0d"
  inset: "#121212"
  rule: "#1f1f1f"
  rule-strong: "#343434"
  seam: "rgba(255, 255, 255, 0.045)"
  ink: "#f0f0f0"
  ink-2: "#b8b8b8"
  ink-3: "#8c8c8c"
  signal-teal: "#14b8a6"
  signal-teal-ink: "#04201c"
  signal-teal-wash: "rgba(20, 184, 166, 0.12)"
  failover-amber: "#f59e0b"
  failover-amber-wash: "rgba(245, 158, 11, 0.12)"
  track-dim: "#4a4a4a"
  lamp-off: "#262626"
  chip: "rgba(255, 255, 255, 0.06)"
  paper-light: "#f6f6f3"
  field-light: "#ecece7"
  inset-light: "#ffffff"
  rule-light: "#dcdcd6"
  rule-strong-light: "#bdbdb5"
  seam-light: "rgba(0, 0, 0, 0.055)"
  ink-light: "#0a0a0a"
  ink-2-light: "#3d3d3d"
  ink-3-light: "#5c5c5c"
  signal-teal-light: "#0f766e"
  signal-teal-ink-light: "#ffffff"
  signal-teal-wash-light: "rgba(15, 118, 110, 0.1)"
  failover-amber-light: "#b45309"
  failover-amber-wash-light: "rgba(180, 83, 9, 0.1)"
  track-dim-light: "#b4b4ad"
  lamp-off-light: "#d6d6d0"
  chip-light: "rgba(0, 0, 0, 0.05)"
typography:
  display:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "clamp(2rem, 1.25rem + 2.6vw, 3.25rem)"
    fontWeight: 700
    lineHeight: 1.04
    letterSpacing: "-0.03em"
  headline:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "clamp(1.5rem, 1.2rem + 1.2vw, 2rem)"
    fontWeight: 700
    lineHeight: 1.15
    letterSpacing: "-0.022em"
  title:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1.0625rem"
    fontWeight: 600
    lineHeight: 1.3
    letterSpacing: "-0.012em"
  body:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1rem"
    fontWeight: 400
    lineHeight: 1.6
    letterSpacing: "normal"
    fontFeature: "cv11"
  body-small:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.9375rem"
    fontWeight: 400
    lineHeight: 1.55
  control:
    fontFamily: "Inter, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.9375rem"
    fontWeight: 600
    lineHeight: 1
  mono:
    fontFamily: "JetBrains Mono, ui-monospace, Menlo, Consolas, monospace"
    fontSize: "0.875em"
    fontWeight: 400
    fontFeature: "zero, tnum"
  label:
    fontFamily: "JetBrains Mono, ui-monospace, Menlo, Consolas, monospace"
    fontSize: "0.75rem"
    fontWeight: 400
    letterSpacing: "normal"
    fontFeature: "zero, tnum"
  plate:
    fontFamily: "JetBrains Mono, ui-monospace, Menlo, Consolas, monospace"
    fontSize: "0.8125rem"
    fontWeight: 700
    letterSpacing: "0.02em"
rounded:
  focus: "2px"
  code: "3px"
  control: "4px"
spacing:
  unit: "4px"
  xs: "0.25rem"
  sm: "0.5rem"
  md: "1rem"
  lg: "1.5rem"
  xl: "2rem"
  section: "4rem"
  tile: "20px"
components:
  button-hairline:
    backgroundColor: "transparent"
    textColor: "{colors.ink}"
    typography: "{typography.control}"
    rounded: "{rounded.control}"
    padding: "0.45rem 0.9rem"
    height: "2.5rem"
  button-hairline-hover:
    backgroundColor: "{colors.chip}"
    textColor: "{colors.ink}"
  nav-link:
    backgroundColor: "transparent"
    textColor: "{colors.ink-2}"
    rounded: "{rounded.control}"
    padding: "0.4rem 0.65rem"
  nav-link-hover:
    backgroundColor: "{colors.chip}"
    textColor: "{colors.ink}"
  scenario-tab:
    backgroundColor: "transparent"
    textColor: "{colors.ink-2}"
    padding: "0.5rem 0.8rem"
  scenario-tab-selected:
    backgroundColor: "{colors.signal-teal-wash}"
    textColor: "{colors.ink}"
  copy-button:
    backgroundColor: "{colors.carbon}"
    textColor: "{colors.ink-2}"
    rounded: "{rounded.code}"
    padding: "0.3rem 0.7rem"
    height: "2.1rem"
  copy-button-done:
    backgroundColor: "{colors.carbon}"
    textColor: "{colors.signal-teal}"
  command-row:
    backgroundColor: "{colors.inset}"
    textColor: "{colors.ink}"
    typography: "{typography.mono}"
    rounded: "{rounded.control}"
    padding: "0.3rem 0.3rem 0.3rem 0.9rem"
  panel:
    backgroundColor: "{colors.field}"
    textColor: "{colors.ink}"
    rounded: "{rounded.control}"
  notice:
    backgroundColor: "{colors.failover-amber-wash}"
    textColor: "{colors.ink-2}"
    rounded: "{rounded.control}"
    padding: "1rem 1.1rem"
---

# Design System: fleet-router

## Overview

**Creative North Star: "The Signal Box Panel"**

The page is a railway block-signalling mimic panel. Client sessions are routes set across block sections, from indexer platforms on one side, through the fleet-router junction, to upstream SHiP stations on the other; a failover is the points thrown to the parallel line while the lamps stay lit. Everything else on the page, the feature legend, the install rows, the config notes and the metrics table, reuses the panel's own vocabulary: the same five state keys, the same hairline seams, the same mono identifiers.

The material is a carbon ground with a slightly lifted panel field, both drawn with 1px hairlines rather than tonal depth. Lines are the whole language: 1px rules separate rows, 2px track strokes carry idle routes, 3px strokes carry a set route, and a 20px seam grid underlies the board. Color is rationed to two signals, teal and amber, and each is paired with a line style so that no state depends on hue alone. Dark is the default; the light scheme is the same panel printed on paper, not a separate design.

The build refused the category arrangement for developer landing pages: no radial glow under the headline, no uniform card grid, no eyebrow kickers, no section numbers, no glass header, no pill controls. Where the direction contract and the build diverged, the build wins and is recorded here (the light panel field shipped as a slightly lighter stone than the contract named).

**Key Characteristics:**
- One strict five-state vocabulary (route set, failover, stale, offline, quarantined), each owning one color and one line style, reused in the board, the legend keys, the feature list, the security notice and the metrics states.
- Hairline construction: 1px rules and 1px seams on a 20px tile module; 2px / 3px track strokes; 1.5px key borders.
- Two signal colors only, teal for set and verified, amber for failover and warning; everything else is a grey ink step.
- Inter 700 tight-tracked for headings, Inter 400 for prose, JetBrains Mono (`zero`, `tnum`, no ligatures) for every identifier, port, range and block number.
- Flat surfaces, 4px control radius, no shadows, no blur, no decorative gradients, no cards.
- Motion is a single stroke-in on load, then hold; state changes are 300ms fades; controls answer in 160ms.

## Colors

A carbon panel with two signal lamps: teal for a route that is set, amber for a route that is failing over or a node that needs watching, with every other surface and text tone a step of grey.

### Primary
- **Signal Teal** (`signal-teal`, light `signal-teal-light`): the lit route. Used for set track strokes, online lamps, set section labels, the `$` prompt in command rows, the selected scenario tab underline and wash, link hover underlines, text selection and the focus ring. Its wash (`signal-teal-wash`) tints the selected scenario tab only.
- **Signal Teal Ink** (`signal-teal-ink`): the only text color allowed on a solid teal fill (used by `::selection`).

### Secondary
- **Failover Amber** (`failover-amber`, light `failover-amber-light`): the thrown points. Used for diverted track strokes, failed (dashed) track strokes, stale lamp rings, the quarantine hatch, the drop mark, failover and stale labels, the security notice border and the copy button's failed state. Its wash (`failover-amber-wash`) fills the security notice only.

### Neutral
- **Carbon** (`carbon`, light `paper-light`): the page ground, the masthead, the panel's lower rail and the copy button face.
- **Field** (`field`, light `field-light`): the panel plate the board is drawn on; the only surface a step above the ground.
- **Inset** (`inset`, light `inset-light`): command rows, code blocks and the SVG station boxes. In dark it sits a step above the field; in light it is white.
- **Rule** (`rule`, light `rule-light`): the quiet 1px divider between rows, table cells, the masthead bottom and the panel rail top.
- **Rule Strong** (`rule-strong`, light `rule-strong-light`): the hairline that draws a control: button borders, the panel border, section-head top rules, SVG box strokes, ticks, the offline lamp ring, the logo separator and the resting link underline.
- **Seam** (`seam`, light `seam-light`): the 20px grid lines inside the panel field.
- **Ink / Ink 2 / Ink 3** (`ink`, `ink-2`, `ink-3` and their `-light` pairs): three text steps. Ink is headings, body and set control labels; Ink 2 is prose in secondary positions, captions, nav links and resting control labels; Ink 3 is mono meta, table heads, notes and the junction box stroke.
- **Track Dim** (`track-dim`, light `track-dim-light`): the idle track stroke, the step-list top rule and the resting key border.
- **Lamp Off** (`lamp-off`, light `lamp-off-light`): the offline lamp and key fill.
- **Chip** (`chip`, light `chip-light`): the translucent hover fill for nav links and hairline buttons, and the inline code background.

### Named Rules
**The Two Lamps Rule.** Teal and amber are the only chromatic colors on the page. Teal means set, online, verified or confirmed; amber means failover, stale, quarantined, failed or a boundary the operator must respect. Neither is decorative; if a surface is not reporting a state, it is grey.

**The Line Carries The State Rule.** Every state pairs its color with a line style so it survives monochrome: route set is a solid 3px teal stroke and a filled teal lamp; failover is a solid 3px amber stroke; stale is a 6/5 dashed dim stroke and a hollow amber ring; offline is a 2/5 dotted amber stroke and a dim filled lamp with a strong-rule ring; quarantined is a 4px 45-degree amber hatch. The legend keys are 10px squares that repeat the same fill and border per state.

**The Printed Panel Rule.** The light scheme swaps every token for its `-light` pair and nothing else. Teal and amber darken for contrast on paper; the inset goes white; structure, weights and strokes are unchanged.

## Typography

**Display Font:** Inter (self-hosted variable woff2, with ui-sans-serif and system-ui fallback)
**Body Font:** Inter
**Label/Mono Font:** JetBrains Mono (self-hosted variable woff2, with ui-monospace, Menlo and Consolas fallback)

**Character:** Engineering plate lettering. Headings are heavy and tightly tracked so they read as stencilled titles; prose is plain Inter with the single-storey `cv11` alternates; anything a machine would emit (ports, ranges, block numbers, flags, metric names, endpoints) is set in mono with slashed zeros and tabular figures, ligatures off.

### Hierarchy
- **Display** (700, `clamp(2rem, 1.25rem + 2.6vw, 3.25rem)`, 1.04, -0.03em): the hero H1 only. Balanced wrapping.
- **Headline** (700, `clamp(1.5rem, 1.2rem + 1.2vw, 2rem)`, 1.15, -0.022em): section titles, set under a strong 1px top rule with 1.25rem of air above the text.
- **Title** (600, 1.0625rem, 1.3, -0.012em): step names, install method names, legend terms (legend terms drop to 1rem), sub-titles.
- **Body** (400, 1rem, 1.6): paragraph text on the carbon ground. The hero lede rises to 1.0625rem in Ink 2 at a 62ch measure.
- **Body Small** (400, 0.9375rem, 1.55 to 1.6): captions, legend and table descriptions, notes, in Ink 2, at 64 to 78ch measures.
- **Control** (600, 0.9375rem, 1): hairline button labels. Nav links are 500 at the same size; scenario tabs 500 at 0.875rem; copy buttons 600 at 0.8125rem.
- **Mono** (400, 0.875em of the surrounding text): inline identifiers; command rows run at 0.9rem and code blocks at 0.8125rem with 1.6 leading.
- **Label** (mono 400, 0.72rem to 0.8125rem, Ink 3): plate meta, legend list items, code-block bar, table column heads (the latter uppercase with 0.04em tracking), terminal step notes.
- **Plate** (mono 700, 0.8125rem, 0.02em, uppercase): the panel title plate name only.
- **Board text** (inside the SVG, px-sized against the viewBox): station and platform names Inter 600 13px, the junction title Inter 700 17px, addresses and ranges mono 10.5px in Ink 3, section labels mono 11px that take the state color.

### Named Rules
**The Machine Speaks Mono Rule.** Any string a program would print (a port, an address, a block number, a flag, a metric, an endpoint path, a config key) is mono with `zero` and `tnum`, in prose and in the board alike. Prose never sets a number a machine owns in Inter.

**The No Kicker Rule.** Headings stand on a rule, not under an eyebrow. The only uppercase on the page is the panel's title plate and the table column heads, both of which are panel furniture, not labels above headlines.

## Layout

The page is a single 1200px column (`min(1200px, 100% - 2 * gutter)`), gutter 1rem below 720px and 2rem above. The masthead is a sticky 4rem bar on the ground, closed by a quiet 1px rule; it holds the two masked brand logos divided by a 1px separator, the section nav, the theme toggle and the GitHub hairline button. In the first viewport the panel fills the column; the hero copy (H1, lede, install command, alternates) sits below it in a 1.2fr / 0.8fr grid from 720px up, and moves above the panel on narrow screens.

Vertical rhythm is on a 4px unit: rem spacing steps are multiples of 0.25rem (0.25, 0.5, 0.75, 1, 1.25, 1.5, 2, 2.5, 3.5, 4rem). Sections open with 3.5rem of space (4rem between consecutive sections) and a strong top rule over the section head, whose measure is capped at 64ch. Inside sections, content is rows divided by quiet 1px rules, never boxes: the feature legend, install rows and config notes are two-column definition rows (`minmax(15rem, 1fr)` label column against a 1.6fr to 2.2fr description column from 720px); the four failover steps are a 2-column grid from 560px and 4 columns from 980px, each step opening with a 2px track-colored top rule and a 10px state key sitting on it; the metrics section is a two-column grid with the five-state list spanning the full width in an auto-fit grid of 13rem columns.

The panel board itself is drawn on a 20px tile module: the seam grid is 20px, and the landscape SVG (viewBox 1200 x 340) places platforms, junction and stations on multiples of that module with rows at 60, 170 and 280. Below 720px the board is a second, separately composed portrait SVG (viewBox 390 x 652) with platforms across the top, the junction mid-board and stations stacked beneath; the panel recomposes rather than scaling the landscape drawing down. At the same breakpoint the scenario switch becomes a 2 x 2 grid and nav links wrap onto their own row under the brands.

Breakpoints: 560px (steps, targets and states go to two columns), 720px (gutter, panel rail, hero, definition rows, metrics and footer go to their desktop grids; portrait board swaps to landscape), 980px (steps to four columns, config to 1.5fr / 1fr).

## Elevation & Depth

There are no shadows. Depth is three tonal steps (ground, field, inset) plus hairlines: the panel is a one-step field inside a strong-rule border, and command rows and code blocks are a further inset step inside their own strong-rule border. Nothing floats; the sticky masthead is separated from scrolled content by its 1px rule and opaque ground, not by a shadow or blur. The one `box-shadow` in the stylesheet is an inset 2px bottom bar on the selected scenario tab, used as an underline, not as elevation.

### Named Rules
**The Hairline Depth Rule.** A surface is raised by changing its tone one step and drawing its edge with a 1px rule. It is never raised with a shadow, a blur, a glass effect or a gradient.

## Shapes

Rectilinear with barely softened corners. Controls, the panel, command rows, code blocks and the notice share a 4px radius; inline code chips, the copy button and the SVG boxes use 3px; the focus ring is 2px outline with 2px offset and a 2px radius. Legend keys are hard 10px squares with a 1.5px border. Lamps are 5px-radius circles. Track paths are stroked polylines with rounded joins and 45-degree diagonals between rows; the drop mark is a 10px X with round caps. The segmented scenario switch is one 4px-radius hairline frame with 1px internal dividers, with the inner buttons square. The 45-degree hatch (6px repeat, 2.5px bar) is the only patterned fill, and it belongs to the quarantined state.

## Components

### Buttons
Hairline, flat and quiet; a button is a label inside a strong-rule frame.
- **Shape:** 4px radius, 1px `rule-strong` border, transparent face.
- **Hairline (the only variant):** Ink text, Inter 600 at 0.9375rem, 0.45rem 0.9rem padding to a 2.5rem minimum height, 0.5rem gap for an inline 16px SVG icon. Used for the masthead GitHub link.
- **Hover:** border steps to Ink 3 and the face takes the Chip wash, over 160ms ease-out. No transform, no shadow.
- **Focus:** 2px teal outline, 2px offset.
- **Theme toggle:** the same frame as a 2.5rem square holding an 18px stroked sun or moon icon in Ink 2, 1.8 stroke, round caps.
- **Nav links:** borderless, Ink 2, 500 weight, 0.4rem 0.65rem padding with the 4px radius; hover brings Ink and the Chip wash.

### Segmented Scenario Switch
One control drives the whole panel: four `role=tab` buttons in a single hairline frame, with arrow-key, Home and End navigation.
- **Frame:** 4px radius, 1px `rule-strong` border, overflow clipped; 1px `rule-strong` dividers between buttons.
- **Tab:** transparent, Ink 2, Inter 500 at 0.875rem, 0.5rem 0.8rem padding. Hover gives Ink and the Chip wash.
- **Selected:** Ink text on the teal wash, with a 2px teal bar along the bottom edge (`inset 0 -2px 0`). Focus ring sits inside the frame (-2px offset, square).
- **Narrow:** a 2 x 2 grid filling the panel width, 0.8125rem labels with short alternates.

### Command Row and Copy Button
A command is a one-line terminal row: prompt, command, copy.
- **Row:** 4px radius, 1px `rule-strong` border, Inset face, mono at 0.9rem, grid of `auto 1fr auto` with a 0.6rem gap, 0.3rem padding opening to 0.9rem on the left. The `$` prompt is teal and not selectable; the command scrolls horizontally without a visible scrollbar.
- **Copy button:** 3px radius, 1px `rule-strong` border, Carbon face, Ink 2 label in Inter 600 at 0.8125rem, 4.5rem minimum width, 2.1rem minimum height. The label is a stack of three spans (Copy / Copied / Failed) sharing one grid cell so the width never changes.
- **States:** hover brings Ink and an Ink 3 border; done turns text and border teal; failed turns them amber; both revert to idle after 1.6s. A visually hidden live region announces the result.
- **Code block:** the same frame around a `pre`, with a mono 0.75rem Ink 3 bar on top naming the file and carrying the copy button, separated by a quiet rule.

### State Keys
The five-state legend swatch, reused wherever a state is named outside the board.
- **Shape:** 10px square, 1.5px border, inline with text at a 0.45rem gap.
- **Route set:** teal fill and border. **Failover:** amber fill and border. **Stale:** transparent with an amber border. **Offline:** Lamp Off fill with a `rule-strong` border. **Quarantined:** amber border over a 45-degree 2px amber hatch.
- **Where:** the panel legend, the feature legend's mono sub-lines, the metrics state list, the security notice and (as a pseudo-element) the top of each failover step.

### Notice
The one bordered call-out, reserved for an operator boundary.
- **Style:** 4px radius, 1px amber border, amber wash face, Ink 2 text at 0.9375rem with a strong Ink lead sentence, a failover key in the left column at a 0.9rem gap, 1rem 1.1rem padding.

### Definition Rows
Lists are rows, not cards: a `dl` or install row divided by quiet 1px rules, with the last row closed by one. The term column carries a 600-weight title and, in the feature legend, a mono 0.75rem sub-line with a state key; the description column is Body Small in Ink 2 at a 70 to 78ch measure. Tables follow the same rule-per-row construction with mono uppercase column heads.

### Signal Box Panel (signature)
A `figure` with four parts, inside a 4px-radius `rule-strong` border on the Field tone with the 20px seam grid.
- **Title plate** (top-left): the product name in uppercase mono 700 at 0.8125rem, followed by mono 0.75rem Ink 3 meta with the listener name, bind address and version as inline code.
- **State legend** (top-right): a mono 0.72rem Ink 3 list of the five keys, in the fixed order route set, failover, stale, offline, quarantined.
- **Board**: two inline SVGs built by the script, landscape for 720px and up and portrait below, each with its own hatch pattern. Boxes are Inset-filled 3px-radius rectangles with a `rule-strong` stroke (the junction box uses an Ink 3 stroke); tracks are 2px `track-dim` polylines at 0.6 opacity when idle; set and diverted tracks are 3px; lamps are 5px circles beside each station; the landscape board adds 1px tick pairs on the station approaches; section labels sit above each station approach in mono 11px and take the state color.
- **Lower rail**: the scenario switch on the left and a Body Small caption on the right (68ch, four lines reserved from 720px so the panel height does not jump), on the Carbon ground above a quiet top rule. The caption is the live region the tabs control.
- **Motion:** on load each track strokes in once with a dash-offset draw over 520ms on `cubic-bezier(0.16, 1, 0.3, 1)`, staggered 110ms in the order indexer, hyperion, atomic, trunk, c, b, a after a 90ms lead; then the board holds. Scenario changes fade stroke, fill and label color over 300ms ease-out and toggle the drop mark's opacity. Under `prefers-reduced-motion` the draw-in is skipped and all transitions are disabled, so the finished panel appears at once.
- **No script:** a short paragraph in Ink 2 describes the mechanism in place of the board.

## Do's and Don'ts

### Do:
- **Do** report every state with the Two Lamps palette and the matching line style: teal solid for set, amber solid for failover, dashed dim for stale, dotted amber for failed, hatch for quarantined, and reuse the 10px key square wherever the state is named in text.
- **Do** build controls as 4px-radius hairline frames with a transparent face, a 160ms ease-out hover that steps the border to Ink 3 and adds the Chip wash, and a 2px teal focus ring.
- **Do** divide lists and tables with quiet 1px rules and open sections with a strong 1px rule over the headline; keep spacing on the 4px unit and the board on the 20px tile.
- **Do** set every identifier, number a machine owns, path and flag in JetBrains Mono with `zero` and `tnum` and ligatures off.
- **Do** keep copy buttons at a fixed width by stacking their state labels in one grid cell.
- **Do** compose a separate portrait board below 720px instead of scaling the landscape drawing.
- **Do** draw motion once on load, hold, and show the finished state immediately under reduced motion.

### Don't:
- **Don't** use pill or capsule controls; the control radius is 4px and the chip radius 3px.
- **Don't** add shadows, blur, glass or radial glow; the only `box-shadow` permitted is the 2px inset underline on a selected segment.
- **Don't** use tonal or decorative gradients; the 45-degree hatch pattern is a state fill, not a gradient, and is reserved for quarantined.
- **Don't** wrap content in cards or card grids; rows on rules are the container.
- **Don't** put eyebrow kickers, uppercase labels or section numbers above headings; uppercase mono belongs to the panel plate and table heads only.
- **Don't** introduce a third chromatic color or use teal and amber for emphasis that is not a state.
- **Don't** set the light scheme with values outside the `-light` token pairs.
