# Tautulli-style Plex.tv authentication and server discovery – reference for implementation

Use this document (and the searches below) to replicate Tautulli’s Plex.tv flow in any app (e.g. PMDA): login via small window + automatic server discovery.

---

## What to search for on the web

1. **Tautulli source code**
   - GitHub: `Tautulli/Tautulli` → file `plexpy/plextv.py`
   - Search for: `get_plextv_pin`, `get_plextv_resources`, `get_plextv_server_list`, `discover`, `get_server_token`

2. **Plex API**
   - PIN flow: “Plex API v2 pins” – POST to create PIN, GET to poll for token
   - Resources: “plex.tv api resources” – list devices/servers with connections
   - Official: `https://plexapi.dev`, `https://www.plexopedia.com`

3. **Concrete URLs**
   - Create PIN: `POST https://plex.tv/api/v2/pins` (optional `?strong=true` for long PIN; without it you get 4-digit code for plex.tv/link)
   - Poll PIN: `GET https://plex.tv/api/v2/pins/{id}`
   - List servers/resources (Tautulli uses this): `GET https://plex.tv/api/resources?includeHttps=1` with header `X-Plex-Token`
   - Alternative server list: `GET https://plex.tv/pms/servers.xml` with `X-Plex-Token`

---

## Process (how Tautulli does it)

### Step 1: Open “small window” for login (PIN flow)

1. App requests a PIN from Plex:
   - `POST https://plex.tv/api/v2/pins`
   - Optional: `?strong=true` for stronger PIN (Tautulli uses `strong=true` in code).
   - Headers: `X-Plex-Product`, `X-Plex-Client-Identifier`, `X-Plex-Device`, etc. (see Plex API docs). No token yet.
2. Response contains:
   - `id` (PIN id)
   - `code` (e.g. 4-digit code for plex.tv/link)
   - `expiresIn` (seconds)
   - (Optional) `authToken` only after user has completed login.
3. App shows the user:
   - The **code** (and optionally a link like `https://app.plex.tv/link` or the `authUrl` from response).
   - User opens that link in a browser, signs in to Plex, and enters the code (or authorizes the app).
4. App **polls** until the PIN is linked:
   - `GET https://plex.tv/api/v2/pins/{id}`
   - When the user has completed authorization, the response includes `authToken`. That is the **account token** to store and use.

### Step 2: Discover servers (“like a champ”)

Tautulli does **not** use `/servers` or `/resources` (root). It uses:

- **Primary:** `GET https://plex.tv/api/resources?includeHttps=1`
  - Headers: `X-Plex-Token: <the_token_from_step_1>`
  - Response: **XML** with structure:
    - `MediaContainer`
      - `Device` (attributes: `clientIdentifier`, `name`, `provides`, `owned`, `presence`, `platform`, …)
        - `Connection` (attributes: `protocol`, `address`, `port`, `uri`, `local`)
  - Filter devices where:
    - `provides` contains `"server"`
    - `owned` = `"1"`
    - (Optional) `presence` = `"1"` to show only currently online servers.
  - For each such device, take its `Connection` nodes and build the list of server URLs (address, port, uri).

- **Alternative (server list):** `GET https://plex.tv/pms/servers.xml` with `X-Plex-Token`
  - Returns XML with `Server` elements (name, address, port, machineIdentifier, etc.).

Important: the path that works for “list my servers” is **`/api/resources`** (with the `api` prefix), not `/resources`. The root `/resources` can return 404 on some Plex versions or configurations.

### Step 3: Optional – server-specific token

For some operations Tautulli gets a **server-specific** token from the same resources response: in the `Device` element that matches the server, it reads `accessToken` (or from user details). For “fetch servers” and picking a server URL, the account token from the PIN flow is enough.

---

## Summary for implementers

| Step            | Tautulli / Plex approach                                                                 |
|-----------------|-------------------------------------------------------------------------------------------|
| Login UI        | Show code + link (e.g. plex.tv/link); user signs in in browser and enters code.          |
| Get token       | POST `/api/v2/pins` → poll GET `/api/v2/pins/{id}` until `authToken` is present.         |
| List servers    | GET `https://plex.tv/api/resources?includeHttps=1` with `X-Plex-Token`, parse XML.       |
| Parse response  | MediaContainer → Device (provides=server, owned=1) → Connection (uri, address, port).    |
| Fallback        | GET `https://plex.tv/pms/servers.xml` or `https://plex.tv/servers.xml?includeLite=1`.    |

Use this process and these URLs in your app (e.g. PMDA) so that “Sign in with Plex.tv” + “Fetch my servers” behaves like Tautulli’s “Plex.tv Authentication” flow and server discovery.
