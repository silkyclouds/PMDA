# Lovable – Prompt: PMDA Settings / Configuration UI/UX

Use this prompt in Lovable to improve the **PMDA Configuration** modal and settings flow. The backend already exposes `GET /api/config` and `PUT /api/config`; the UI must display **real configured values** from the API and apply the following UX rules.

---

## 1. Load and display real configuration

- When the user opens the **Settings** (configuration wizard), the app must call **GET /api/config** and **fill every form field** with the returned values (Plex URL, token, database path, section IDs, paths, scan options, OpenAI key/model, Discord webhook, etc.).
- Do **not** show only placeholder or default values if the API returns data. Empty or default values are only for when no config exists yet.

---

## 2. Optional / advanced options

- If a setting is **optional** (e.g. Discord webhook, MusicBrainz, fallback models, or any “optional” flag in the config schema), **do not display it by default** in the main form.
- Add a small **“Advanced”** link or button at the bottom of each step (or a dedicated “Show optional options” control) that **expands** to reveal these optional fields.
- This keeps the main flow simple; power users can expand to see and set optional items.

---

## 3. Tooltips for every configurable item

- For **each** configurable field (Plex URL, token, database path, section IDs, scan threads, OpenAI key, etc.), add a **tooltip** (e.g. a small “?” icon next to the label) that shows a short description of what the setting does.
- Prefer a consistent pattern: **label** + **?** icon; on hover (or click on mobile), show the description in a tooltip/popover.
- If a config schema or XML exists (e.g. Unraid template with `<Config Description="...">`), use those **Description** values as the tooltip text so the user understands each option without reading docs.

---

## 4. Modal: glass effect and stacking

- When the **Configuration** modal is open:
  - The **backdrop** (overlay) must use a strong **glass effect**: clearly **blurred** background (e.g. `backdrop-blur-md` or stronger), and a semi-transparent overlay so the content behind does not “mix” visually with the modal. The background should look distinctly behind and out of focus.
  - The **modal panel** must **always** be on top (higher `z-index` than the overlay and the page). The background must never scroll or render **over** the modal while it is open.
- Ensure the modal has a solid or high-opacity background so it is readable and clearly in the foreground.

---

## 5. Keyboard navigation

- **Escape**: Close the configuration modal.
- **Arrow Left**: In the wizard, go to the **previous** step (e.g. from “Paths” to “Plex”).
- **Arrow Right**: In the wizard, go to the **next** step (e.g. from “Plex” to “Paths”).
- Ensure focus is trapped inside the modal while open (optional but recommended for accessibility).

---

## 6. Sensitive fields: mask and reveal

- **API keys, tokens, and passwords** (e.g. Plex token, OpenAI API key, Discord webhook URL) must **not** appear in clear text by default.
  - Show them as **masked** (e.g. `••••••••` or password-type input).
  - Provide a **“Show” / “Hide”** control (e.g. an **eye icon** button) so the user can toggle visibility when they need to check or copy the value.
- This applies to: Plex Token, OpenAI API Key, Discord Webhook (and any similar secrets).

---

## 7. Test and autodetection in settings

- The settings UI must allow the user to **test** connections (e.g. **Test Plex**, **Test OpenAI**) and see success/failure clearly.
- Add the ability to **run autodetection** from the Web UI, similar to what the backend does at startup:
  - **Force autodetection** of folders / path bindings (PATH_MAP).
  - **Force autodetection** of library / section IDs (e.g. from Plex API).
- After running autodetection, show the **detected values** in the form (and optionally allow the user to save them). The user must be able to **validate visually** that bindings and library IDs are correct before saving.

---

## 8. Example config descriptions (for tooltips)

You can use descriptions like these (or from your XML/schema) for the tooltips:

- **Plex Server URL**: “Your Plex server address, without trailing slash (e.g. http://192.168.1.1:32400).”
- **Plex Token**: “Authentication token for the Plex API. Find it in Plex → Settings → Account.”
- **Database Path (Container)**: “Path to the Plex database directory inside the container (e.g. /database).”
- **Section IDs**: “Comma-separated library section IDs for music libraries (e.g. 1,2,3).”
- **OpenAI API Key**: “Your OpenAI API key for LLM-based best-edition selection.”
- **Discord Webhook**: “Optional Discord webhook URL for completion notifications.”

Optional / advanced fields can use descriptions that mention they are optional.

---

## Summary checklist

- [ ] Load config from GET /api/config and populate all fields.
- [ ] Optional options hidden by default; “Advanced” expands them.
- [ ] Tooltip (?) for each configurable field with a clear description.
- [ ] Modal: strong blur/glass on backdrop; modal always on top.
- [ ] Escape = close modal; Arrow Left/Right = previous/next wizard step.
- [ ] Sensitive fields masked; eye icon to show/hide.
- [ ] Test buttons (Plex, OpenAI) and autodetection (paths, library IDs) with visual result in the UI.
