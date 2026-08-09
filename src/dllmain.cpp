//! IntegratedStorage (C++). Author: Sarfflow
//
// Cross-camp build/craft on a Palworld guild: build or craft at ANY of your guild's camps using materials
// stored in ANY other same-guild camp. One DLL, role-gated to all three ends (dedicated / host-SP / remote
// client). Native ItemStackInfo is NEVER mutated, so native Quick Stack + the Item Retrieval Device keep
// working. Architecture:
//
//   SERVER (authority)  — a ~8s DISCOVERY RECONCILE enumerates every guild chest from the map-object
//                         manager AND every base camp (incl. EMPTY ones) and cross-registers each guild
//                         chest's container into every same-guild camp's storage module. That lets the
//                         native build/craft flow CONSUME cross-camp (and, on a host/SP authority, the
//                         native collector already reads the merged containers -> correct display for free).
//
//   REMOTE CLIENT       — can't see far-camp containers, so it DISPLAYS the guild total by minting local
//                         item slots and array-swapping them into a spare inventory container ("cont5")
//                         only for the duration of the native material scan (3 AOB-located detours). The
//                         per-item pool comes over a custom TRANSPORT CHANNEL (below), never the ISI.
//
//   TRANSPORT CHANNEL   — demand-driven, event-driven, ISI-free: the client tracks its current camp via the
//                         OnEnterBaseCamp hook (no polling), fires a light trigger RPC from on_update (a safe
//                         top-level tick), the server resolves that client's camp, reads GROUND-TRUTH
//                         container contents for (guild - own), and replies over an engine RPC; the client
//                         parses it into the pool. No FindAllOf in any per-frame path.
//
// All patch sites are located by unique AOB signature at load (survives address-shifting game updates).

#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <winsock2.h>   // MUST precede Windows.h (external TCP transport channel)
#include <ws2tcpip.h>
#include <Windows.h>
#include <cstdint>
#include <cstring>
#include <vector>
#include <string>
#include <fstream>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <thread>
#include <mutex>
#include <atomic>
#include <memory>

#include <Mod/CppUserModBase.hpp>
#include <DynamicOutput/DynamicOutput.hpp>
#include <Unreal/NameTypes.hpp>
#include <Unreal/Core/Containers/Array.hpp>
#include <Unreal/UObjectGlobals.hpp>
#include <Unreal/UObject.hpp>
#include <Unreal/UClass.hpp>
#include <Unreal/UFunction.hpp>
#include <Unreal/UFunctionStructs.hpp> // UnrealScriptFunctionCallableContext / GetParams<>
#include <Unreal/World.hpp>            // UWorld (GetWorld for world-change detection)

#pragma push_macro("ensure")
#pragma push_macro("check")
#undef ensure
#undef check
#include <polyhook2/Detour/x64Detour.hpp>
#pragma pop_macro("check")
#pragma pop_macro("ensure")

using namespace RC;
using namespace RC::Unreal;

struct RawTArray { uint8_t* data; int32_t num; int32_t max; };

// ============================================================================
//  DIAGNOSTIC ISOLATION FLAGS (uncomment ONE to isolate a subsystem for testing)
// ============================================================================
// ISOLATE_NO_SWAP: keeps CH RPC channel fully active (normal request/reply), but
//   makes injectMinted skip the actual SlotArray pointer swap. The minted slots
//   are still created/stamped/rooted, but never written into the live container.
//   If interaction failure STILL happens -> CH RPC channel is the cause.
//   If failure STOPS -> injectMinted swap is the cause.
//#define ISOLATE_NO_SWAP

// ============================================================================
//  Config (config.txt beside the mod; parsed at load — see loadConfig)
// ============================================================================
//! Pre-stable build -> verbose logging DEFAULTS ON so field issues are diagnosable; users can quiet it and
//! tune the reconcile cadence via config.txt without rebuilding. Each key keeps its default if absent.
static bool     g_verbose      = true;     // verbose [ISGATE] diagnostics
static uint64_t g_reconcileMs  = 8000;     // authority: discovery-reconcile cadence (min 500)
static uint64_t g_isiRefreshMs = 1500;     // (reserved; config-compat) remote-client refresh cadence
//! EXTERNAL TRANSPORT CHANNEL (TCP) — replaces the debug-RPC carrier (Debug_CheatCommand) which
//! saturated the PlayerController reliable buffer and froze ALL native interactions (summon/teleport/
//! eat for 90s–23min). The channel runs OUTSIDE the UE net driver on its own socket + thread, so it
//! exerts ZERO reliable-buffer pressure. Same DLL ships to every end; role (IsServer) picks listener
//! (authority) vs connector (remote client). The host/SP authority never needs the channel — it reads
//! the cross-registered containers natively.
static bool          g_extEnabled = true;   // external TCP channel master switch
static uint16_t      g_extPort    = 27500;  // TCP port: server listens here, client connects here
static std::wstring  g_extHost    = L"";    // client only: server IP/host to connect to ("" = no client channel)
//! Layer 2 delta sync still applies over TCP — keeps each reply <300B (only changed items).
static bool     g_chDelta        = true;   // send incremental pool updates (IS2|) instead of always-full (IS1|)
static uint64_t g_chFullSyncMs   = 3600000;  // L2: full sync interval — 1h (delta-only in practice; initial sync still FULL)

// ============================================================================
//  Struct offsets (ref/sdk/SDK)
// ============================================================================
static const uintptr_t OFF_INV_MYINFO   = 0x100;  // UPalPlayerInventoryData: FPalPlayerDataInventoryInfo (CommonContainerId @ +0x00)
static const uintptr_t OFF_INV_MULTI    = 0x190;  // UPalPlayerInventoryData: UPalItemContainerMultiHelper*
static const uintptr_t OFF_MULTI_CONTS  = 0x38;   // UPalItemContainerMultiHelper: TArray<UPalItemContainer*>
static const uintptr_t OFF_CONT_ID      = 0x38;   // UPalContainerBase.ID (FPalContainerId, 16 bytes)
static const uintptr_t OFF_CONT_SLOTS   = 0x70;   // UPalItemContainer: TArray<UPalItemSlot*>
static const uintptr_t OFF_CONT_OWNER   = 0xF8;   // UPalItemContainer.OwnerMapObjectInstanceId (FGuid; nonzero => camp-building storage)
static const uintptr_t OFF_SLOT_CONT_ID = 0x11C;  // UPalItemSlot.ContainerId (FPalContainerId, 16 bytes)
static const uintptr_t OFF_SLOT_ITEMID  = 0x12C;  // UPalItemSlot: FPalItemId.StaticId (FName)
static const uintptr_t OFF_SLOT_COUNT   = 0x154;  // UPalItemSlot.StackCount (int32)
static const uintptr_t OFF_SLOT_INDEX   = 0x118;  // UPalItemSlot.SlotIndex (int32)

static bool guidZero(const uint8_t* g) { for (int i = 0; i < 16; ++i) if (g[i]) return false; return true; }
static void hexOf(const uint8_t* g, wchar_t out33[33]) {   // 16-byte FGuid -> 32 lowercase hex chars
    static const wchar_t* H = L"0123456789abcdef";
    for (int b = 0; b < 16; ++b) { out33[b*2] = H[g[b] >> 4]; out33[b*2+1] = H[g[b] & 15]; }
    out33[32] = 0;
}
static bool hexToGuid(const std::wstring& hex, uint8_t out16[16]) {   // 32 hex chars -> 16 bytes; false if malformed
    if (hex.size() < 32) return false;
    auto hv = [](wchar_t c)->int { if (c>=L'0'&&c<=L'9') return c-L'0'; if (c>=L'a'&&c<=L'f') return c-L'a'+10;
                                   if (c>=L'A'&&c<=L'F') return c-L'A'+10; return -1; };
    for (int b = 0; b < 16; ++b) {
        int hi = hv(hex[b*2]), lo = hv(hex[b*2+1]);
        if (hi < 0 || lo < 0) return false;
        out16[b] = (uint8_t)((hi << 4) | lo);
    }
    return true;
}

//! The guild pool the client displays = {item id -> count}. Filled from the transport channel (parsePoolReply),
//! NOT from the ISI. poolGet is used only by diagnostics.
static std::vector<std::pair<FName, int32_t>> g_pool;
static int32_t poolGet(const FName& id) { for (auto& kv : g_pool) if (kv.first == id) return kv.second; return -1; }

// fwd decls (defined further down)
static UObject* findCommonContainer();
static UObject* findDonorContainer();
static void mintPoolSlots();
static void checkWorld(void* anyObj);
static bool clientInCamp();        // PURE-READ local in-camp test (defined near chClientTrigger, needs OFF_PAWN_CAMPCHECK)
static bool clientInCampStable();  // A1: debounced "confirmed in camp" (hysteresis over clientInCamp)
static bool chClientTrigger(std::wstring& outHex);

// ============================================================================
//  Role (single signal: UPalUtility::IsServer, verified in-game)
// ============================================================================
//! The SAME dll ships to both ends. The client-DISPLAY half (the 3 injection detours) runs ONLY on a pure
//! remote client — an authority (dedicated / host / standalone) reads the server-cross-registered containers
//! natively and needs no display injection. So: DISPLAY <=> !IsServer ; SERVER work <=> IsServer.
//! Pitfall: the TITLE menu is a local STANDALONE world where IsServer=true — so role must be computed from an
//! IN-GAME context (FindFirstOf(PalPlayerCharacter): false on a joined client, true on host/dedicated), never
//! the menu. Role is fixed once connected -> computed once and cached.
static int g_isSrv  = -1;   // -1 unknown, 0 remote client, 1 authority (dedicated/host/standalone)
static int g_isDedi = -1;   // -1 unknown, 0 no, 1 dedicated server (informational: distinguishes dedicated vs host/SP)
static UObject* g_palUtil = nullptr;
static bool callUtilBool(const CharType* fnName, void* wc, bool* faulted = nullptr) {
    if (faulted) *faulted = false;
    __try {
        if (!wc) return false;
        if (!g_palUtil) g_palUtil = UObjectGlobals::StaticFindObject<UObject*>(nullptr, nullptr, STR("/Script/Pal.Default__PalUtility"));
        if (!g_palUtil) return false;
        UFunction* fn = g_palUtil->GetFunctionByNameInChain(fnName);
        if (!fn) return false;
        struct { UObject* WorldContext; bool Ret; uint8_t pad[7]; } p{}; p.WorldContext = (UObject*)wc;
        g_palUtil->ProcessEvent(fn, &p);
        return p.Ret;
    } __except (EXCEPTION_EXECUTE_HANDLER) {
        //! An AV here (e.g. `wc` is a half-destroyed PalPlayerCharacter whose world context is being torn down)
        //! is NOT "IsServer returned false" — it's a transient fault. Report it via `faulted` so callers
        //! (ensureRole / the ROLE watchdog) can distinguish a real role change from a swallowed fault. Without
        //! this, the watchdog treated every transient AV as a genuine server->client role flip and nuked all
        //! state, permanently re-deriving a dedicated server as a CLIENT (2026-08-02 outage: 20:50:49).
        if (faulted) *faulted = true;
        return false;
    }
}
static void ensureRole(void* wc) {
    if (g_isSrv >= 0 || !wc) return;
    //! Probe IsDedicatedServer FIRST — it is process-fixed (set by -server on the command line) and does
    //! NOT depend on world/replication state, so it is far more reliable than IsServer right after a reset.
    g_isDedi = callUtilBool(STR("IsDedicatedServer"), wc) ? 1 : 0;
    bool faulted = false;
    g_isSrv  = callUtilBool(STR("IsServer"), wc, &faulted) ? 1 : 0;
    //! If the IsServer probe FAULTED (transient AV on a half-destroyed WorldContext), do NOT commit a wrong
    //! "client" (g_isSrv=0) role — leave it unknown and let the next probe retry. Committing 0 here on a
    //! dedicated server would make every server-side guard short-circuit until a world change.
    //! v4.0.2: pinning g_isSrv=-1 on a fault starves the ENTIRE mod (on_update returns before HEARTBEAT),
    //! the reconnect-after-disconnect outage. On a dedicated process the role is process-fixed, so fall
    //! back to authority; non-dedicated cannot be guessed, so stay unknown and retry next tick.
    if (faulted) {
        if (g_isDedi == 1) g_isSrv = 1; else g_isSrv = -1;
        return;
    }
    //! A dedicated-server PROCESS is always authority. IsServer can transiently read false right after a
    //! world change / reset (world mid-replication). Trusting that misread demotes the dedicated server to
    //! CLIENT and kills ALL server-side work (reconcile/reply stop, calls=0). 2026-08-04 13:38:40 outage:
    //! a world-change reset re-derived a dedicated server as CLIENT (server=0 dedicated=1 -> CLIENT).
    //! Override: on a dedicated server, IsServer=false is treated as a transient misread.
    if (g_isDedi == 1 && g_isSrv == 0) {
        g_isSrv = 1;
        Output::send(STR("[ISGATE] ROLE override: IsServer=false on DEDICATED server -> treated as DEDICATED (server)\n"));
        return;
    }
    if (faulted) { g_isSrv = -1; return; }
    Output::send(STR("[ISGATE] ROLE server={} dedicated={} -> {}\n"), g_isSrv, g_isDedi,
        g_isSrv == 0 ? STR("CLIENT (display)") : (g_isDedi == 1 ? STR("DEDICATED (server)") : STR("HOST/SP (server)")));
}
//! run the client-display half? ONLY on a pure remote client. Role comes from a PalPlayerCharacter (NOT a
//! hook's WorldContext — that resolves to a world reading IsServer=TRUE even on a remote client). Cheap:
//! scans only until the role is cached.
static bool isClient(void* = nullptr) {
    if (g_isSrv < 0) ensureRole(UObjectGlobals::FindFirstOf(STR("PalPlayerCharacter")));
    return g_isSrv == 0;
}

// ============================================================================
//  Client DISPLAY — mint local slots + transient array-swap into cont5
// ============================================================================
//! We show an UNBOUNDED guild total by minting one local UPalItemSlot per pool item via the game's native
//! factory (UPalItemUtility::CreateLocalItemSlot), collecting them into our own buffer, and TRANSIENTLY
//! pointing a spare inventory container's ItemSlotArray at that buffer during the native material scan
//! (restore immediately after). Fresh local slots have no home container, so they can't be double-counted;
//! the swap is a cheap pointer assignment per scan; minting is throttled to reply-time (g_poolDirty).
static void* g_lastWc = nullptr;                 // a WorldContext from the detours (for the slot factory)
static std::vector<UObject*> g_mintedSlots;      // our minted slots (append source)
static RawTArray g_savedDonorArr{};              // cont5's real {data,num,max}, saved across the swap
static std::vector<UObject*> g_swapBuf;          // [cont5's real slots...] + [minted slots...], cont5 points here during a scan
static UObject* g_swapDonor = nullptr;           // the EXACT donor injectMinted swapped — restore must target THIS one, not a raced g_donorCont
static bool g_swapped2 = false;
static bool g_poolDirty = false;                 // reply arrived -> re-mint on the next detour tick
static bool g_needTrigger = false;               // set by camp-enter / menu-open hooks; on_update fires the network trigger from a SAFE context
static uint64_t g_lastFetchOk = 0;               // A4: GetTickCount64 of the last successful pool fetch (drives on_update self-heal refresh)
static bool g_inCampStable = false;              // A1: debounced "confirmed in camp" (hysteresis over clientInCamp)
static int   g_inCampStreak = 0;                 // A1: consecutive disagreeing samples toward flipping g_inCampStable
static uint64_t g_inCampLastSampleAt = 0;        // A1: timestamp of the last streak-advancing sample
static bool     g_inCampHook       = false;      // v4.0.2: authoritative in-camp state from OnEnter/OnExitBaseCamp hooks (the NowInsideBaseCampID poll reads zero while idle in-camp)
static bool     g_inCampHookKnown  = false;      // v4.0.2: has an enter/exit hook fired yet? (before that, fall back to the poll)
static uint64_t g_lastEnterAt      = 0;          // v4.0.4: last OnEnterBaseCamp time - exit within ~3s is suppressed (boundary enter/exit multicast pair)
static bool g_mintStampDirty = true;             // C1: minted slots need ContainerId/SlotIndex re-stamping
static int   g_lastStampRealNum = -1;            // C1: cont5 real slot count at last stamp (append-position stability)
//! Channel back-pressure. Triggering used to piggyback on the collector detour (shared with the ammo HUD),
//! so the reliable RPC channel got driven during normal play and saturated (input froze, no disconnect).
//! Now: fire at most one trigger per CH_MIN_INTERVAL_MS, never a second while the previous reply is still
//! outstanding, and only while the player is inside a camp. CH_REPLY_TIMEOUT_MS force-releases the in-flight
//! lock if a reply is ever dropped, so the channel can't wedge shut.
static const uint64_t CH_MIN_INTERVAL_MS  = 3000;
static const uint64_t CH_REPLY_TIMEOUT_MS = 5000;
static bool     g_awaitingReply = false;         // a trigger was sent, reply not yet received (in-flight guard)
static uint64_t g_lastTrigAt    = 0;             // GetTickCount64 of the last trigger we sent
static uint64_t g_myCalls   = 0;                 // requests WE queued onto the TCP channel (client-side counter)
static int      g_consecMiss = 0;                // consecutive unanswered requests (channel health; 0 = last reply arrived)
static int      g_missLogged = 0;                // suppress repeated miss-warning logs until recovery
static UObject* g_common    = nullptr;           // cached local player's Common container
static UObject* g_donorCont = nullptr;           // cached donor container (cont5)
//! Layer 2: per-CONNECTION snapshot for delta sync. Held inside NetPeer::snap (one per TCP client), it
//! stores the last-sent pool (item-name -> count) so the server computes IS2| deltas.
struct ClientSnap {
    std::map<std::wstring, int32_t> lastPool;    // last-sent pool (name -> count)
    uint64_t lastFullMs = 0;                      // GetTickCount64 of last FULL (IS1|) sync
    bool initialized = false;                     // has this client ever received data?
    bool wantFull = true;                         // force a FULL sync on first request / reconnect
};
//! NOTE: the local PlayerController / PlayerInventoryData are NOT cached — a cached UObject* can dangle when
//! the game frees/recreates it without a world change, and reading a stale inventory's container array
//! AV'd in findCommonContainer (crash_2026_07_26). These are all resolved live via FindFirstOf at their call
//! sites, which only sit on cold or throttled paths (trigger path, or clientInCamp's 300ms cache).
static UObject* g_itemUtilCdo = nullptr;
static UFunction* g_createSlotFn = nullptr;
static UObject* createLocalSlot(void* wc, const FName& id, int32_t count) {
    if (!g_itemUtilCdo) g_itemUtilCdo = UObjectGlobals::StaticFindObject<UObject*>(nullptr, nullptr, STR("/Script/Pal.Default__PalItemUtility"));
    if (!g_itemUtilCdo) return nullptr;
    if (!g_createSlotFn) g_createSlotFn = g_itemUtilCdo->GetFunctionByNameInChain(STR("CreateLocalItemSlot"));
    if (!g_createSlotFn) return nullptr;
    struct { UObject* WorldContext; FName Id; int32_t Stack; UObject* Ret; } p{};
    p.WorldContext = (UObject*)wc; p.Id = id; p.Stack = count; p.Ret = nullptr;
    g_itemUtilCdo->ProcessEvent(g_createSlotFn, &p);
    return p.Ret;
}
static void mintPoolSlots() {
    //! GC-ROOT the minted slots (fix A): CreateLocalItemSlot returns UObjects referenced ONLY from our
    //! std::vector, which is invisible to UE's GC -> the collector would eventually GC them, and a later scan
    //! that reads the freed slot builds an item-info element with a garbage TSharedPtr -> CTD when the game
    //! destroys that array (0xc0000005 releasing rcx=0xbe.., seen in the UECC dump). SetRootSet() keeps them
    //! alive; unroot the previous batch first so refreshes don't leak.
    for (UObject* s : g_mintedSlots) if (s) s->ClearRootSet();
    g_mintedSlots.clear();
    g_mintStampDirty = true;   // C1: the slot set changed -> re-stamp on the next injectMinted swap
    if (!g_lastWc || g_pool.empty()) return;
    g_mintedSlots.reserve(g_pool.size());
    for (auto& kv : g_pool) {
        if (kv.second <= 0) continue;
        std::wstring nm = kv.first.ToString();                       // fix B: skip empty/None ids the client can't resolve
        if (nm.empty() || nm == STR("None")) continue;
        UObject* s = createLocalSlot(g_lastWc, kv.first, kv.second);  // returns nullptr for an unknown item -> skipped below
        if (s) { s->SetRootSet(); g_mintedSlots.push_back(s); }
    }
}
static int g_injDiag = 0;
//! APPEND (not replace): cont5 is the player's PRECIOUS-ITEMS container (implants / collectibles). The old
//! code swapped cont5's ENTIRE slot array out for the minted buffer; if restoreMinted was ever skipped (a
//! throwing/AV-ing native scan, or a raced g_donorCont), cont5 stayed detached and the player's implants
//! VANISHED until a re-login rebuilt them from the save. Instead we keep every real slot and APPEND the
//! minted ones after them: [real 0..num-1][minted...]. cont5's real items contribute 0 to build recipes
//! (they aren't recipe materials — the reason cont5 was chosen as donor), so the count is unchanged, while
//! the implant/craft UI still sees the real slots. Even a leaked swap can no longer hide or lose them.
static void injectMinted() {
    if (g_swapped2 || g_mintedSlots.empty()) return;
#ifdef ISOLATE_NO_SWAP
    return;   // diagnostic: CH RPC traffic continues, but the live container is never mutated
#endif
    //! OUT-OF-CAMP GATE: cross-camp items must ONLY appear while the player stands INSIDE a camp. The pool
    //! persists in g_mintedSlots after a reply, and the not-in-camp pool-clear in chClientTrigger only runs
    //! while g_needTrigger is set — so once a pool is fetched, walking out and opening a menu still showed the
    //! cross-camp items. Gate the append here (covers every scan path, not just menu-open): if not in a camp,
    //! drop the stale pool (unroot the minted slots) so the menu shows OWN items only. Re-entering a camp
    //! (hkEnterCamp) re-fetches + re-mints. After this clears, later scans early-return on g_mintedSlots.empty().
    //!
    //! A1: gate on the DEBOUNCED clientInCampStable() (not the raw clientInCamp()). The raw NowInsideBaseCampID
    //! field transiently reads zero during camp-boundary recalc, and a single miss here used to wipe the pool
    //! mid-build with no auto-recovery -> "materials suddenly show only the current camp". The debounce absorbs
    //! the glitch; a genuine exit still settles within ~1-2s.
    if (!clientInCampStable()) {
        for (UObject* s : g_mintedSlots) if (s) s->ClearRootSet();
        g_mintedSlots.clear();
        g_pool.clear();
        g_needTrigger = true;   // A3: ensure the pool self-heals — on_update's in-camp refresh re-fetches it
        if (g_verbose && g_injDiag < 8) { ++g_injDiag; Output::send(STR("[ISGATE] INJ skip: out of camp -> dropped stale pool\n")); }
        return;
    }
    UObject* dc = g_donorCont; if (!dc) return;
    RawTArray* slots = (RawTArray*)((uint8_t*)dc + OFF_CONT_SLOTS);
    if (slots->num < 0 || slots->num > 4096) return;             // sanity: don't touch a garbage array
    g_savedDonorArr = *slots;                                    // save cont5's real {data,num,max}
    //! combined = real slots (kept, so implants stay visible + in-scan) then minted slots. Stamp each minted
    //! slot so the scan accepts it as belonging to cont5: ContainerId (@0x11C) = cont5's ID (@0x38), and give
    //! it a SlotIndex (@0x118) matching its position in the combined array.
    uint8_t* cid = (uint8_t*)dc + OFF_CONT_ID;
    //! C1: only (re)stamp minted slots when something changed. A minted slot's ContainerId/SlotIndex only need
    //! rewriting when the pool was re-minted (new UObjects) or cont5's REAL slot count shifted (which moves every
    //! minted slot's append position). In steady build the pool + cont5 are stable for seconds at a time, so this
    //! skips a per-slot memcpy+write on the (frequent) collector scan while still re-pointing the slot array.
    bool needStamp = g_mintStampDirty || (g_savedDonorArr.num != g_lastStampRealNum);
    g_swapBuf.clear();
    g_swapBuf.reserve((size_t)g_savedDonorArr.num + g_mintedSlots.size());
    for (int i = 0; i < g_savedDonorArr.num; ++i) g_swapBuf.push_back(((UObject**)g_savedDonorArr.data)[i]);
    for (UObject* s : g_mintedSlots) {
        if (!s) continue;
        if (needStamp) {
            std::memcpy((uint8_t*)s + OFF_SLOT_CONT_ID, cid, 16);
            *(int32_t*)((uint8_t*)s + OFF_SLOT_INDEX) = (int32_t)g_swapBuf.size();
        }
        g_swapBuf.push_back(s);
    }
    if (needStamp) { g_mintStampDirty = false; g_lastStampRealNum = g_savedDonorArr.num; }
    slots->data = (uint8_t*)g_swapBuf.data();
    slots->num  = (int32_t)g_swapBuf.size();
    slots->max  = (int32_t)g_swapBuf.size();
    g_swapDonor = dc;                                            // pin: restore MUST target this exact donor
    g_swapped2  = true;
    if (g_verbose && g_injDiag < 8) { ++g_injDiag; Output::send(STR("[ISGATE] INJDIAG appended {} minted after {} real slots\n"), (int)g_mintedSlots.size(), g_savedDonorArr.num); }
}
static void restoreMinted() {
    if (!g_swapped2) return;
    if (g_swapDonor) { RawTArray* slots = (RawTArray*)((uint8_t*)g_swapDonor + OFF_CONT_SLOTS); *slots = g_savedDonorArr; }
    g_swapDonor = nullptr;
    g_swapped2  = false;
}
static int g_injectDepth = 0;   // re-entrancy guard: only the OUTERMOST detour swaps/restores (they nest)

//! Called at the top of each detour (CHEAP, no scans, no network). ONLY re-mints when the pool just changed.
//! It no longer flags the network trigger: triggering is driven by genuine menu-open / camp-enter hooks. The
//! collector is shared with the ammo HUD, so the old detour-gap heuristic here fired the channel during
//! normal play and saturated the reliable RPC queue. The transient array-swap is done by injectMinted/
//! restoreMinted around the native scan, so g_swapped2 is false in here.
static void clientDetourTick() {
    if (g_poolDirty) { mintPoolSlots(); g_poolDirty = false; }
}

//! Find the LOCAL player's Common container by matching MyInventoryInfo.CommonContainerId against each
//! aggregated container's UPalContainerBase.ID. Client has exactly one inventory.
static UObject* findCommonContainer() {
    //! SEH: the aggregated container array can momentarily hold a dangling entry (a container freed mid-mutation
    //! on the game thread while this runs on the UE4SS thread); reading cont+0x38 then AVs (crash_2026_07_26).
    //! POD-only body -> safe to guard; a fault just yields "no common container this tick".
    __try {
        UObject* inv = UObjectGlobals::FindFirstOf(STR("PalPlayerInventoryData"));   // ALWAYS fresh -> current live object (never cache: a stale ptr dangles)
        if (!inv) return nullptr;
        uint8_t* ip = (uint8_t*)inv;
        uint8_t* commonId = ip + OFF_INV_MYINFO;
        UObject* multi = *(UObject**)(ip + OFF_INV_MULTI);
        if (!multi) return nullptr;
        RawTArray* conts = (RawTArray*)((uint8_t*)multi + OFF_MULTI_CONTS);
        if (!conts->data || conts->num <= 0 || conts->num > 64) return nullptr;
        for (int i = 0; i < conts->num; ++i) {
            UObject* cont = ((UObject**)conts->data)[i];
            if (!cont) continue;
            if (std::memcmp((uint8_t*)cont + OFF_CONT_ID, commonId, 16) == 0) return cont;
        }
        return nullptr;
    } __except (EXCEPTION_EXECUTE_HANDLER) { return nullptr; }
}
//! The player's largest non-Common inventory container (= the ~230-slot "cont5"), which is NOT part of the
//! material-count set (proven: injecting it yields CountItemNum 0). SAFE donor: player-owned, never scanned
//! independently, so our transiently-swapped slots can't be double-counted.
static UObject* findDonorContainer() {
    __try {   // same dangling-container guard as findCommonContainer (reads c+0x70)
        UObject* inv = UObjectGlobals::FindFirstOf(STR("PalPlayerInventoryData"));   // ALWAYS fresh -> current live object (never cache)
        if (!inv) return nullptr;
        UObject* multi = *(UObject**)((uint8_t*)inv + OFF_INV_MULTI);
        if (!multi) return nullptr;
        RawTArray* conts = (RawTArray*)((uint8_t*)multi + OFF_MULTI_CONTS);
        if (!conts->data || conts->num <= 0 || conts->num > 64) return nullptr;
        UObject* best = nullptr; int bestN = -1;
        for (int i = 0; i < conts->num; ++i) {
            UObject* c = ((UObject**)conts->data)[i];
            if (!c || c == g_common) continue;
            int nsl = ((RawTArray*)((uint8_t*)c + OFF_CONT_SLOTS))->num;
            if (nsl > bestN) { bestN = nsl; best = c; }
        }
        return best;
    } __except (EXCEPTION_EXECUTE_HANDLER) { return nullptr; }
}

// ============================================================================
//  AOB signature scanning (survives address-shifting game updates)
// ============================================================================
//! Locate each site at load by a wildcarded byte signature (relative/RIP operands wildcarded, struct offsets
//! kept). Uniqueness in the module IS the correctness guard: exactly ONE match -> use it; ZERO or MANY ->
//! skip loudly. If a function's BYTES change (not just its address), regenerate its sig from a fresh analysis.
struct Sig { std::vector<uint8_t> b; std::vector<uint8_t> wild; };
static Sig parseSig(const char* s) {
    Sig sig;
    auto hv = [](char c)->int { if (c>='0'&&c<='9') return c-'0'; if (c>='A'&&c<='F') return c-'A'+10;
                                if (c>='a'&&c<='f') return c-'a'+10; return 0; };
    for (const char* p = s; *p; ) {
        if (*p == ' ') { ++p; continue; }
        if (*p == '?') { sig.b.push_back(0); sig.wild.push_back(1); p += (p[1]=='?') ? 2 : 1; }
        else { sig.b.push_back((uint8_t)((hv(p[0])<<4)|hv(p[1]))); sig.wild.push_back(0); p += 2; }
    }
    return sig;
}
struct ExecRange { const uint8_t* start; size_t size; };
static std::vector<ExecRange> g_exec;
static void initExecRanges(uintptr_t base) {
    auto* dos = (IMAGE_DOS_HEADER*)base;
    auto* nt  = (IMAGE_NT_HEADERS*)(base + dos->e_lfanew);
    auto* sec = IMAGE_FIRST_SECTION(nt);
    for (int i = 0; i < nt->FileHeader.NumberOfSections; ++i)
        if (sec[i].Characteristics & IMAGE_SCN_MEM_EXECUTE)
            g_exec.push_back({ (const uint8_t*)(base + sec[i].VirtualAddress), (size_t)sec[i].Misc.VirtualSize });
}
// scan every executable section; returns the unique match address (0 if none). *count is capped at 2 so
// callers can distinguish not-found (0) from ambiguous (>=2).
static uintptr_t scanSig(const Sig& s, int* count) {
    const size_t n = s.b.size(); *count = 0;
    if (n == 0) return 0;
    const uint8_t b0 = s.b[0]; const bool w0 = s.wild[0] != 0;
    uintptr_t found = 0; int c = 0;
    for (auto& r : g_exec) {
        if (r.size < n) continue;
        const uint8_t* p = r.start; const size_t last = r.size - n;
        for (size_t i = 0; i <= last; ++i) {
            if (!w0 && p[i] != b0) continue;
            size_t j = 1;
            for (; j < n; ++j) if (!s.wild[j] && p[i+j] != s.b[j]) break;
            if (j == n) { if (!found) found = (uintptr_t)(p + i); if (++c >= 2) { *count = c; return found; } }
        }
    }
    *count = c; return found;
}

//! The 3 material-scan functions the remote client detours: collector (craft + build-confirm haves),
//! catalog (build-open availability), placement (per-recipe placement counter). Real injected numbers make
//! the native gates pass on their own (bisection 2026-07-17 removed the old "optimistic gate" overrides), so
//! only these 3 remain. Re-verify unique on every game update.
static const char* SIG_COLLECTOR = "48 89 5C 24 08 48 89 6C 24 18 48 89 74 24 20 48 89 54 24 10 57 41 56 41 57 48 83 EC 60 41 0F B6 E9 4D 8B F0 48 8B FA 48 8B F1 48 8B D1 48 8D 4C 24 48 E8 ?? ?? ?? ?? 48 8D 44 24 38 48 89 44 24 30";
static const char* SIG_CATALOG   = "48 89 5C 24 20 55 56 57 41 54 41 55 41 56 41 57 48 8D 6C 24 F0 48 81 EC 10 01 00 00 48 8B 05 ?? ?? ?? ?? 48 33 C4 48 89 45 00 4D 8B E8 4C 89 44 24 50 48 8B DA 33 FF 48 89 7D B0 48 89 7D B8 48 89 7D D0";
static const char* SIG_PLACEMENT = "40 55 53 56 57 41 54 41 55 41 56 41 57 48 8D 6C 24 D8 48 81 EC 28 01 00 00 48 8B 05 ?? ?? ?? ?? 48 33 C4 48 89 45 10 4D 8B F9 4C 89 4C 24 58 4D 8B E0 48 8B DA 4C 89 45 88 33 FF 48 89 7D C0 48 89 7D C8";
struct Target { const CharType* name; const char* sig; uint64_t tramp; PLH::x64Detour* det; bool hooked; uintptr_t addr; };
static Target g_collect = { STR("collector"), SIG_COLLECTOR, 0, nullptr, false, 0 };
static Target g_7d0     = { STR("catalog"),   SIG_CATALOG,   0, nullptr, false, 0 };
static Target g_ac0     = { STR("placement"), SIG_PLACEMENT, 0, nullptr, false, 0 };

//! Each detour (remote client only): tick + transient array-swap cont5's slots -> our minted buffer around
//! the native scan, restore after. Outermost-only guard because these functions nest. On the authority the
//! isClient() guard makes it a pure pass-through (the server reads merged containers natively).
typedef int64_t(__fastcall* tCollect)(void*, void*, void*, uint8_t);   // (ctx, reqIds, OUT, type)
static int64_t __fastcall hkCollect(void* c, void* r, void* o, uint8_t t) {
    if (!isClient(c)) return reinterpret_cast<tCollect>(g_collect.tramp)(c, r, o, t);
    g_lastWc = c; checkWorld(c); clientDetourTick();
    bool outer = (g_injectDepth == 0);
    if (outer) injectMinted();
    ++g_injectDepth;
    int64_t x = 0;
    __try { x = reinterpret_cast<tCollect>(g_collect.tramp)(c, r, o, t); }
    __finally { --g_injectDepth; if (outer) restoreMinted(); }   // ALWAYS restore cont5, even if the scan throws/AVs
    return x;
}
typedef int64_t(__fastcall* t7d0)(void*, void*, void*);   // catalog (ctx, containers, OUT)
static int64_t __fastcall hk7d0(void* a1, void* a2, void* o) {
    if (!isClient(a1)) return reinterpret_cast<t7d0>(g_7d0.tramp)(a1, a2, o);
    g_lastWc = a1; checkWorld(a1); clientDetourTick();
    bool outer = (g_injectDepth == 0);
    if (outer) injectMinted();
    ++g_injectDepth;
    int64_t x = 0;
    __try { x = reinterpret_cast<t7d0>(g_7d0.tramp)(a1, a2, o); }
    __finally { --g_injectDepth; if (outer) restoreMinted(); }   // ALWAYS restore cont5, even if the scan throws/AVs
    return x;
}
typedef int64_t(__fastcall* tAc0)(void*, void*, void*, void*);   // placement (ctx, containers, reqIds, OUT)
static int64_t __fastcall hkAc0(void* a1, void* a2, void* r, void* o) {
    if (!isClient(a1)) return reinterpret_cast<tAc0>(g_ac0.tramp)(a1, a2, r, o);
    g_lastWc = a1; checkWorld(a1); clientDetourTick();
    bool outer = (g_injectDepth == 0);
    if (outer) injectMinted();
    ++g_injectDepth;
    int64_t x = 0;
    __try { x = reinterpret_cast<tAc0>(g_ac0.tramp)(a1, a2, r, o); }
    __finally { --g_injectDepth; if (outer) restoreMinted(); }   // ALWAYS restore cont5, even if the scan throws/AVs
    return x;
}

// ============================================================================
//  SERVER — discovery reconcile + container cross-registration (authority only)
// ============================================================================
static const wchar_t*  SRV_CHEST_CLASS  = L"PalMapObjectItemChestModel";
static const wchar_t*  SRV_FOOD_CLASS   = L"PalMapObjectPalFoodBoxModel";
static const uintptr_t OFF_CAMP_MODULES = 0x180;   // UPalBaseCampModel.ModuleArray (TArray<module*>)
static const uintptr_t OFF_CAMP_GROUPID = 0xE4;    // UPalBaseCampModel.GroupIdBelongTo (FGuid) -> guild key
static const uintptr_t OFF_CAMP_ID      = 0x58;    // UPalBaseCampModel.ID (FGuid) = the camp's own id
static const uintptr_t OFF_CONT_MGR_MAP = 0x98;   // UPalItemContainerManager.ItemContainerMap_InServer (TMap)

struct GuildData {
    std::unordered_set<UObject*> storages, models;
    std::unordered_map<UObject*, UObject*> modelCamp;    // chest model  -> its owning camp
    std::unordered_map<UObject*, UObject*> storageCamp;  // storage module -> its owning camp
};
static std::unordered_map<std::wstring, GuildData> g_guilds;
static std::unordered_map<std::wstring, UObject*> g_instToCamp;  // chest map-object instance-id (hex) -> its camp
static std::unordered_map<std::wstring, UObject*> g_instToCont;   // B2: chest instance-id (hex) -> its UPalItemContainer (rebuilt each reconcile)
static std::unordered_map<std::wstring, UObject*> g_campIdToCamp; // B3: camp-id (hex of FGuid@0x58) -> camp object (rebuilt each reconcile)
static std::unordered_map<UObject*, std::unordered_set<UObject*>> g_registered;  // B1: storage -> {models already cross-registered into it this world}
static bool g_srvInjecting = false;   // re-entrancy guard (our cross-register calls re-fire the storage events)

//! walk the UClass chain for an exact class name
static bool srvClassIs(UObject* o, const wchar_t* name) {
    UStruct* c = (UStruct*)o->GetClassPrivate();
    for (int i = 0; c && i < 24; ++i) { if (c->GetName() == name) return true; c = c->GetSuperStruct(); }
    return false;
}
//! guild key = the camp's GroupIdBelongTo FGuid (16 bytes) read raw as an 8-wchar string key
static std::wstring srvGuildKey(UObject* camp) {
    return std::wstring((const wchar_t*)((uint8_t*)camp + OFF_CAMP_GROUPID), 8);
}
//! call a one-UObject-arg UFunction by name (OnAvailableConcreteModel_ServerInternal)
static void srvCall1(UObject* obj, const CharType* fnName, UObject* model) {
    UFunction* fn = obj->GetFunctionByNameInChain(fnName);
    if (!fn) return;
    struct { UObject* Model; } p{ model };
    obj->ProcessEvent(fn, &p);
}
//! a chest's CURRENT owning camp (survives camp re-association). ProcessEvent GetBaseCampModelBelongTo.
static UObject* srvCampModelOf(UObject* chest) {
    UFunction* fn = chest->GetFunctionByNameInChain(STR("GetBaseCampModelBelongTo"));
    if (!fn) return nullptr;
    struct { UObject* Ret; } p{};
    chest->ProcessEvent(fn, &p);
    return p.Ret;
}
//! a camp's storage module by walking its ModuleArray (no scan)
static UObject* srvStorageOf(UObject* camp) {
    RawTArray* mods = (RawTArray*)((uint8_t*)camp + OFF_CAMP_MODULES);
    if (!mods->data || mods->num <= 0 || mods->num > 64) return nullptr;
    for (int i = 0; i < mods->num; ++i) { UObject* m = ((UObject**)mods->data)[i]; if (m && srvClassIs(m, L"PalBaseCampModuleItemStorage")) return m; }
    return nullptr;
}

//! DISCOVERY RECONCILE (authority; ~8s correctness pass). Rebuilds guild state from GROUND TRUTH and
//! cross-registers every guild chest's model into every same-guild camp's storage module so the native
//! build/craft flow can CONSUME cross-camp. Two enumerations:
//!   (a) every chest concrete model from the UPalMapObjectManager (TMap @0x310, raw sparse-array walk, no
//!       FindAllOf) -> group by CURRENT camp + guild; also record instance-id -> camp for the channel read.
//!   (b) EVERY base camp incl. EMPTY ones (FindAllOf) -> add its storage to its guild bucket so an empty
//!       camp is a cross-registration TARGET. Without (b) an empty camp is never discovered (discovery is
//!       chest-driven) and building there fails — the root of "works at a stocked camp, not an empty one".
//! Re-find the manager each pass (never cache -> no dangling across world change). Idempotent.
static uint64_t g_lastReconcile = 0;
static int g_recLog = 0;
static uint64_t g_lastRoleCheck = 0;   // (b) role-watchdog probe cadence (on_update; role-independent)
static int g_roleFalseCount = 0;       // consecutive IsServer=false readings before committing to a role reset
static int g_errLog = 0;               // always-on error/diagnostic counter (NOT gated by g_verbose)
static void srvDiscoverReconcileInner() {
    UObject* mgr = UObjectGlobals::FindFirstOf(STR("PalMapObjectManager"));
    if (!mgr) return;
    uint8_t* mm = (uint8_t*)mgr + 0x310;                        // MapObjectConcreteModelMapForServer (TMap)
    uint8_t* elems  = *(uint8_t**)(mm + 0x00);                  // sparse-array element buffer
    int32_t  maxIdx = *(int32_t*)(mm + 0x08);                   // slots incl. holes (== NumBits)
    uint32_t* words = *(uint32_t**)(mm + 0x20); if (!words) words = (uint32_t*)(mm + 0x10);   // allocation bits
    if (!elems || maxIdx <= 0 || maxIdx > 1000000) return;
    std::unordered_map<std::wstring, GuildData> fresh;
    std::unordered_map<std::wstring, UObject*> freshInst;
    std::unordered_map<std::wstring, UObject*> freshCampId;   // B3: camp-id (hex) -> camp object
    int chests = 0;
    for (int32_t i = 0; i < maxIdx; ++i) {
        if (((words[i >> 5] >> (i & 31)) & 1u) == 0) continue;                 // skip free slots
        uint8_t*  keyId = elems + (size_t)i * 0x20 + 0x00;                     // TPair::Key = FGuid instance id
        UObject* model  = *(UObject**)(elems + (size_t)i * 0x20 + 0x10);       // TPair::Value = concrete model
        if (!model) continue;
        const bool isChest = srvClassIs(model, SRV_CHEST_CLASS);
        if (!isChest && !srvClassIs(model, SRV_FOOD_CLASS)) continue;
        UObject* camp = srvCampModelOf(model); if (!camp) continue;
        GuildData& g = fresh[srvGuildKey(camp)];
        //! Both chests AND food boxes are cross-registered: some build recipes require food items, and the
        //! user wants those pullable from any same-guild camp's food box. Previously only chests were in
        //! g.models (the `if (isChest)` guard), so food boxes were discovered but never registered -> building
        //! at camp A couldn't consume food stored in camp B's food box.
        g.models.insert(model); g.modelCamp[model] = camp;
        UObject* st = srvStorageOf(camp); if (st) { g.storages.insert(st); g.storageCamp[st] = camp; }
        wchar_t ih[33]; hexOf(keyId, ih); freshInst[ih] = camp;
        wchar_t ch[33]; hexOf((uint8_t*)camp + OFF_CAMP_ID, ch); freshCampId[ch] = camp;
        ++chests;
        //! [ProbeChest] 共享池探测: 每个普通箱子 model 的类链/候选函数/容器槽位数
        if (g_verbose && chests <= 8) {
            UStruct* mc = (UStruct*)model->GetClassPrivate();
            const wchar_t* mcName = mc ? mc->GetName().c_str() : L"?";
            const wchar_t* candFns[] = {
                L"GetItemContainerAccess", L"GetItemChestContainerAccess",
                L"GetItemContainerModule", L"GetItemContainer",
                L"GetContainer", L"GetItemContainer_ItemContainerAccessInterface",
            };
            std::wstring fnList;
            for (auto fn : candFns) {
                if (model->GetFunctionByNameInChain(fn)) { fnList += fn; fnList += L" "; }
            }
            std::wstring chain;
            for (UStruct* cc = mc; cc && chain.size() < 200; cc = cc->GetSuperStruct()) {
                chain += cc->GetName(); chain += L" <- ";
            }
            // 原版容器槽位数: 从 ItemContainerMap_InServer 反查 (与 B4 同款 TMap walk, 先放这里探测一次)
            int32_t origSlots = -1;
            UObject* contMgr0 = UObjectGlobals::FindFirstOf(STR("BP_PalItemContainerManager_C"));
            if (!contMgr0) contMgr0 = UObjectGlobals::FindFirstOf(STR("PalItemContainerManager"));
            if (contMgr0) {
                uint8_t* cm = (uint8_t*)contMgr0 + OFF_CONT_MGR_MAP;
                uint8_t* cElems  = *(uint8_t**)(cm + 0x00);
                int32_t  cMaxIdx = *(int32_t*)(cm + 0x08);
                uint32_t* cWords = *(uint32_t**)(cm + 0x20); if (!cWords) cWords = (uint32_t*)(cm + 0x10);
                if (cElems && cMaxIdx > 0 && cMaxIdx < 1000000 && cWords) {
                    for (int32_t ci = 0; ci < cMaxIdx; ++ci) {
                        if (((cWords[ci >> 5] >> (ci & 31)) & 1u) == 0) continue;
                        UObject* cont = *(UObject**)(cElems + (size_t)ci * 0x20 + 0x10);
                        if (!cont) continue;
                        uint8_t* cp = (uint8_t*)cont;
                        if (guidZero(cp + OFF_CONT_OWNER)) continue;
                        if (std::memcmp(cp + OFF_CONT_OWNER, keyId, 16) == 0) {
                            origSlots = ((RawTArray*)(cp + OFF_CONT_SLOTS))->num;
                            break;
                        }
                    }
                }
            }
            Output::send(STR("[ProbeChest] model class={} origSlots={} inst={}\n"), mcName, (int)origSlots, ih);
            Output::send(STR("[ProbeChest]   chain: {}\n"), chain.c_str());
            Output::send(STR("[ProbeChest]   cand fns: {}\n"), fnList.c_str());
        }
    }
    int campsSeen = 0;   // (b) every camp, incl. empty ones, becomes a cross-registration target
    //! B4 — enumerate ALL camps via PalBaseCampManager native API instead of FindAllOf("PalBaseCampModel").
    //! GetBaseCampIds() returns every camp's FGuid in one ProcessEvent call; TryGetModel() resolves each
    //! GUID to its PalBaseCampModel (O(1) hash lookup inside the engine vs O(all UObjects) FindAllOf scan).
    //! Falls back to FindAllOf if the manager or its functions aren't found (game update / different build).
    { bool b4ok = false;
      UObject* campMgr = UObjectGlobals::FindFirstOf(STR("BP_PalBaseCampManager_C"));
      if (!campMgr) campMgr = UObjectGlobals::FindFirstOf(STR("PalBaseCampManager"));
      UFunction* getIdsFn = campMgr ? campMgr->GetFunctionByNameInChain(STR("GetBaseCampIds")) : nullptr;
      UFunction* tryGetFn  = campMgr ? campMgr->GetFunctionByNameInChain(STR("TryGetModel"))   : nullptr;
      if (campMgr && getIdsFn && tryGetFn) {
          struct { RawTArray OutIds; } idP{};                    // TArray<FGuid> output (zeroed)
          campMgr->ProcessEvent(getIdsFn, &idP);
          if (idP.OutIds.data && idP.OutIds.num > 0 && idP.OutIds.num < 100000) {
              b4ok = true;
              for (int32_t ci = 0; ci < idP.OutIds.num; ++ci) {
                  uint8_t* gid = idP.OutIds.data + (size_t)ci * 16;   // each FGuid is 16 bytes
                  struct { uint8_t Id[16]; UObject* Out; bool Ret; } tp{};
                  std::memcpy(tp.Id, gid, 16);
                  campMgr->ProcessEvent(tryGetFn, &tp);
                  if (!tp.Ret || !tp.Out) continue;
                  UObject* camp = tp.Out;
                  GuildData& g = fresh[srvGuildKey(camp)];
                  UObject* st = srvStorageOf(camp); if (st) { g.storages.insert(st); g.storageCamp[st] = camp; }
                  wchar_t ch[33]; hexOf((uint8_t*)camp + OFF_CAMP_ID, ch); freshCampId[ch] = camp;
                  ++campsSeen;
              }
          }
      }
      if (!b4ok) {
          if (g_verbose && g_recLog < 10) Output::send(STR("[ISGATE] B4 fallback: PalBaseCampManager API unavailable -> FindAllOf(PalBaseCampModel)\n"));
          std::vector<UObject*> camps; UObjectGlobals::FindAllOf(STR("PalBaseCampModel"), camps);
          for (UObject* camp : camps) { if (!camp) continue;
              GuildData& g = fresh[srvGuildKey(camp)];
              UObject* st = srvStorageOf(camp); if (st) { g.storages.insert(st); g.storageCamp[st] = camp; }
              wchar_t ch[33]; hexOf((uint8_t*)camp + OFF_CAMP_ID, ch); freshCampId[ch] = camp;
              ++campsSeen; }
      } }
    //! B2 — build instance-id -> container map ONCE per reconcile so srvBuildForCamp (per client request) can
    //! resolve a chest's container directly instead of FindAllOf("PalItemContainer") on EVERY request
    //! (O(all UObjects) each). Only camp-storage containers carry a non-zero OwnerMapObjectInstanceId (@0xF8);
    //! player inventories are skipped. Both maps below are read on the same game thread in srvBuildForCamp.
    std::unordered_map<std::wstring, UObject*> freshCont;
    //! B4 — read ItemContainerMap_InServer (TMap at OFF_CONT_MGR_MAP on PalItemContainerManager) directly
    //! instead of FindAllOf("PalItemContainer"). The TMap maps ContainerId -> PalItemContainer*; we walk its
    //! sparse-array backing (same layout as MapObjectManager's TMap at 0x310: element stride 0x20, key at +0x00,
    //! value pointer at +0x10) and read each container's OwnerMapObjectInstanceId (@0xF8) to build the
    //! instance-id -> container map. Falls back to FindAllOf if the manager or TMap is invalid.
    { bool b4c = false;
      UObject* contMgr = UObjectGlobals::FindFirstOf(STR("BP_PalItemContainerManager_C"));
      if (!contMgr) contMgr = UObjectGlobals::FindFirstOf(STR("PalItemContainerManager"));
      if (contMgr) {
          uint8_t* cm = (uint8_t*)contMgr + OFF_CONT_MGR_MAP;          // ItemContainerMap_InServer TMap
          uint8_t* cElems  = *(uint8_t**)(cm + 0x00);                   // sparse-array element buffer
          int32_t  cMaxIdx = *(int32_t*)(cm + 0x08);                    // slots incl. holes
          uint32_t* cWords = *(uint32_t**)(cm + 0x20); if (!cWords) cWords = (uint32_t*)(cm + 0x10);
          if (cElems && cMaxIdx > 0 && cMaxIdx < 1000000 && cWords) {
              b4c = true;
              for (int32_t ci = 0; ci < cMaxIdx; ++ci) {
                  if (((cWords[ci >> 5] >> (ci & 31)) & 1u) == 0) continue;       // skip free slots
                  UObject* cont = *(UObject**)(cElems + (size_t)ci * 0x20 + 0x10); // TPair::Value
                  if (!cont) continue;
                  uint8_t* cp = (uint8_t*)cont;
                  if (guidZero(cp + OFF_CONT_OWNER)) continue;
                  wchar_t ih[33]; hexOf(cp + OFF_CONT_OWNER, ih); freshCont[ih] = cont;
              }
          }
      }
      if (!b4c) {
          if (g_verbose && g_recLog < 10) Output::send(STR("[ISGATE] B4 fallback: ItemContainerMap unavailable -> FindAllOf(PalItemContainer)\n"));
          std::vector<UObject*> conts; UObjectGlobals::FindAllOf(STR("PalItemContainer"), conts);
          for (UObject* c : conts) { if (!c) continue; uint8_t* cp = (uint8_t*)c;
              if (guidZero(cp + OFF_CONT_OWNER)) continue;
              wchar_t ih[33]; hexOf(cp + OFF_CONT_OWNER, ih); freshCont[ih] = c; }
      } }
    //! B1 — DIFF the cross-registration. Prune g_registered down to the storages still live this pass (so a
    //! destroyed camp doesn't leak entries), then register only the (storage,model) pairs we have NOT already
    //! registered this world. Steady state (no new chest/camp) -> ZERO ProcessEvent calls per reconcile instead
    //! of O(storages*models). Re-registration is idempotent and we never un-register, which matches the original
    //! "all same-guild camps can consume all same-guild chests" semantics; a chest that moves camps simply gets
    //! registered into its NEW non-owning camps the first time it appears there.
    { std::unordered_map<UObject*, std::unordered_set<UObject*>> liveReg;
      for (auto& gkv : fresh) for (UObject* st : gkv.second.storages) { auto it = g_registered.find(st); if (it != g_registered.end()) liveReg[st] = std::move(it->second); }
      g_registered = std::move(liveReg); }
    g_srvInjecting = true;   // CONSUME: register each guild chest into every OTHER same-guild camp's storage
    for (auto& gkv : fresh) { GuildData& g = gkv.second;
        for (UObject* st : g.storages) { UObject* sc = g.storageCamp[st];
            std::unordered_set<UObject*>& done = g_registered[st];
            for (UObject* mo : g.models) {
                if (g.modelCamp[mo] == sc) continue;                       // own camp -> already native there
                if (done.count(mo)) continue;                               // B1: already registered this pair -> skip the ProcessEvent
                srvCall1(st, STR("OnAvailableConcreteModel_ServerInternal"), mo);
                done.insert(mo);
            } } }
    g_srvInjecting = false;
    g_guilds = std::move(fresh);
    g_instToCamp = std::move(freshInst);
    g_instToCont = std::move(freshCont);
    g_campIdToCamp = std::move(freshCampId);
    if (g_verbose && g_recLog < 200) { ++g_recLog; Output::send(STR("[ISGATE] SRV discover: chests={} camps={} guilds={} inst={} campIds={}\n"), chests, campsSeen, (int)g_guilds.size(), (int)g_instToCamp.size(), (int)g_campIdToCamp.size()); }
    //! [ProbeChest] StaticConstructObject 创建独立 UPalItemContainer 测试 (一次性, 验证共享池容器可创建)
    static bool s_scoTested = false;
    if (!s_scoTested) {
        s_scoTested = true;
        UObject* outer = UObjectGlobals::FindFirstOf(STR("PalItemContainerManager"));
        UStruct* contClass = nullptr;
        if (outer) {
            UStruct* mc = (UStruct*)outer->GetClassPrivate();
            // 找一个 PalItemContainer 的 CDO/类: 从 manager 的 TMap 里已有的容器拿 Class
            uint8_t* cm = (uint8_t*)outer + OFF_CONT_MGR_MAP;
            uint8_t* cElems  = *(uint8_t**)(cm + 0x00);
            int32_t  cMaxIdx = *(int32_t*)(cm + 0x08);
            uint32_t* cWords = *(uint32_t**)(cm + 0x20); if (!cWords) cWords = (uint32_t*)(cm + 0x10);
            if (cElems && cMaxIdx > 0 && cMaxIdx < 1000000 && cWords) {
                for (int32_t ci = 0; ci < cMaxIdx && !contClass; ++ci) {
                    if (((cWords[ci >> 5] >> (ci & 31)) & 1u) == 0) continue;
                    UObject* cont = *(UObject**)(cElems + (size_t)ci * 0x20 + 0x10);
                    if (cont) contClass = (UStruct*)cont->GetClassPrivate();
                }
            }
            if (contClass) {
                try {
                    Unreal::FStaticConstructObjectParameters params{ (Unreal::UClass*)contClass, (Unreal::UObject*)outer };
                    UObject* created = Unreal::UObjectGlobals::StaticConstructObject(params);
                    Output::send(STR("[ProbeChest] StaticConstructObject -> {:#x} (class {})\n"),
                        (uintptr_t)created, created ? ((UStruct*)created->GetClassPrivate())->GetName().c_str() : L"NULL");
                } catch (...) {
                    Output::send(STR("[ProbeChest] StaticConstructObject threw\n"));
                }
            } else {
                Output::send(STR("[ProbeChest] no existing container class found\n"));
            }
        } else {
            Output::send(STR("[ProbeChest] PalItemContainerManager not found for SCO test\n"));
        }
    }
}
//! (a) SEH wrapper: the reconcile raw-walks the map-object TMap + every camp's ModuleArray + live container
//! slots. A pointer freed mid-walk (object destroyed on the game thread) AVs, and WITHOUT a guard that fault left
//! g_instToCamp/g_instToCont UN-updated (the std::move at the end never ran) — but worse, an AV here used to take
//! the whole server-side channel down. Crucially the inner only commits its fresh maps at the very END (via
//! std::move), so a guarded fault simply KEEPS THE PREVIOUS VALID STATE — srvBuildForCamp keeps answering with
//! last-good data instead of going empty for everyone. The wrapper has no C++ objects with destructors, so it
//! compiles under /EHsc. g_srvInjecting is forced back off in the handler in case the fault struck mid-register.
static void srvDiscoverReconcile() {
    if (g_isSrv != 1) return;
    __try { srvDiscoverReconcileInner(); }
    __except (EXCEPTION_EXECUTE_HANDLER) {
        g_srvInjecting = false;
        if (g_errLog < 64) { ++g_errLog; Output::send(STR("[ISGATE] SRV discover: AV guarded -> kept previous guild state\n")); }
    }
}

// ============================================================================
//  Transport channel — client<->server pool delivery (ISI-free, demand-driven)
// ============================================================================
//! The remote client cannot read far-camp containers, so the server delivers the per-camp guild pool over a
//! custom channel that NEVER touches the native ItemStackInfo aggregate (mutating it broke Quick Stack + the
//! Item Retrieval Device, and it doesn't reliably replicate anyway). The carrier is an EXTERNAL TCP socket on
//! its own thread — fully outside the UE net driver. The old carrier was the Debug_CheatCommand reliable RPC
//! pair on the PlayerController; its ~7KB reply saturated the controller's reliable buffer and froze ALL native
//! interactions (summon/teleport/eat) for 90s–23min. TCP exerts zero reliable-buffer pressure.
//!
//! Flow (demand-driven): the client reads its OWN camp GUID (NowInsideBaseCampID off the pawn's
//! InsideBaseCampCheckComponent) and sends it as the request from on_update (game thread). The server looks the
//! camp up BY THAT ID (no connection->player->camp reverse-mapping), builds (guild - that camp) from GROUND-TRUTH
//! container contents, and replies. The client parses the reply into g_pool. Item ids travel as strings (FName
//! indices are process-local) and are rebuilt with FName(str) — matches the recipe ids exactly.
//! Framing: one message per line ('\n' terminator) — requests and replies never contain a newline.
static const wchar_t*  CH_SENTINEL     = L"IS1|";     // reply   payload tag: IS1|id:cnt,id:cnt,  (FULL pool)
static const wchar_t*  CH_DELTA_TAG    = L"IS2|";     // reply   payload tag: IS2|id:cnt,id:cnt,  (DELTA — changed items only; 0 = removed)
static const wchar_t*  CH_REQ_SENTINEL = L"ISREQ|";   // request payload tag: ISREQ|<32-hex campGuid>
static const uintptr_t OFF_PAWN_CAMPCHECK = 0xC08;   // APalPlayerCharacter.InsideBaseCampCheckComponent
static const uintptr_t OFF_CHK_CAMPID     = 0xC0;    // UPalInsideBaseCampCheckComponent.NowInsideBaseCampID (FGuid)
//! server: find the base camp whose OWN id (@0x58) matches the client-supplied camp GUID. No player/connection
//! reverse-mapping — the requester told us its camp directly.
//! B3 — uses g_campIdToCamp (rebuilt each reconcile) instead of FindAllOf("PalBaseCampModel") on EVERY client
//! request. FindAllOf is O(all UObjects); the cache lookup is O(1). The cache is rebuilt every reconcile (~8s)
//! on this same thread, so a newly-created camp is at most one reconcile cycle stale — the client retries
//! after CH_MIN_INTERVAL_MS anyway.
static UObject* srvCampByIdInner(const uint8_t* campGuid16) {
    wchar_t hex[33]; hexOf(campGuid16, hex);
    auto it = g_campIdToCamp.find(hex);
    return (it != g_campIdToCamp.end()) ? it->second : nullptr;
}
//! (a) SEH wrapper: a dangling camp pointer mid-FindAllOf (an object freed on the game thread) would AV on the
//! memcmp read; guard it so a fault yields "not found this tick" instead of wedging the server request handler.
static UObject* srvCampById(const uint8_t* campGuid16) {
    __try { return srvCampByIdInner(campGuid16); }
    __except (EXCEPTION_EXECUTE_HANDLER) { return nullptr; }
}
//! server: build "IS1|id:cnt,..." = the contents of every chest in the requester's GUILD but NOT in the
//! requester's camp (= guild - own; the client shows its own camp natively). GROUND TRUTH: reads real
//! UPalItemContainer ItemSlotArrays. Each camp-storage container carries OwnerMapObjectInstanceId (@0xF8) =
//! its chest's instance id; g_instToCamp (built in the reconcile) maps that id -> the chest's current camp.
static void srvBuildForCampInner(UObject* camp, std::wstring& out) {
    out = CH_SENTINEL;
    if (!camp) return;
    const std::wstring playerGuild = srvGuildKey(camp);
    std::vector<std::pair<FName, int64_t>> total;
    //! B2 — iterate the (small) instance->camp map (only guild chests) and resolve the container via the
    //! instance->container map built in the reconcile, instead of FindAllOf("PalItemContainer") on EVERY client
    //! request (O(all UObjects) each). Both maps are rebuilt every reconcile (~8s) on this same (game) thread.
    int scanned = 0;
    for (auto& kv : g_instToCamp) {
        UObject* ccamp = kv.second;
        if (ccamp == camp) continue;                                             // own camp -> exclude
        if (srvGuildKey(ccamp) != playerGuild) continue;                         // different guild -> skip
        auto cit = g_instToCont.find(kv.first); if (cit == g_instToCont.end()) continue;  // container gone / not yet mapped
        UObject* c = cit->second; if (!c) continue;
        uint8_t* cp = (uint8_t*)c;
        RawTArray* slots = (RawTArray*)(cp + OFF_CONT_SLOTS);
        if (!slots->data || slots->num <= 0 || slots->num > 4096) continue;
        ++scanned;
        for (int i = 0; i < slots->num; ++i) {
            UObject* slot = ((UObject**)slots->data)[i]; if (!slot) continue;
            int32_t cnt = *(int32_t*)((uint8_t*)slot + OFF_SLOT_COUNT); if (cnt <= 0) continue;
            FName id = *(FName*)((uint8_t*)slot + OFF_SLOT_ITEMID);
            bool f = false; for (auto& t : total) if (t.first == id) { t.second += cnt; f = true; break; }
            if (!f) total.emplace_back(id, (int64_t)cnt);
        }
    }
    int items = 0;
    for (auto& t : total) {
        int64_t d = t.second; if (d <= 0) continue; if (d > 0x7fffffffLL) d = 0x7fffffffLL;
        out += t.first.ToString() + L":" + std::to_wstring(d) + L","; ++items;
    }
    if (g_verbose) Output::send(STR("[ISGATE] CH build: otherCamp-conts={} items={} len={}\n"), scanned, items, (int)out.size());
}
//! (a) SEH wrapper: srvBuildForCamp reads live ItemSlotArrays on every client request; a container freed mid-scan
//! would AV on the slot read. Guard it so a fault yields an empty (sentinel-only) reply instead of crashing the
//! request path. The wrapper holds no C++ objects with destructors (`out` is a ref) so it compiles under /EHsc;
//! on a guarded fault the caller's string is reset to the sentinel and that client simply re-fetches on its next
//! trigger (A2 self-heal).
static void srvBuildForCamp(UObject* camp, std::wstring& out) {
    __try { srvBuildForCampInner(camp, out); }
    __except (EXCEPTION_EXECUTE_HANDLER) {
        out = CH_SENTINEL;
        if (g_errLog < 64) { ++g_errLog; Output::send(STR("[ISGATE] CH build: AV guarded -> empty reply\n")); }
    }
}
//! Layer 2 — wrap srvBuildForCamp with DELTA encoding. Parses the full IS1| pool into a name→count map,
//! compares against the per-client snapshot, and emits either:
//!   IS1|...  (FULL — first request, >60s since last full, or delta disabled)
//!   IS2|...  (DELTA — only items whose count CHANGED; name:0 means removed)
//! The snapshot is updated each call so the next delta is relative to this send.
//! Layer 2 — wrap srvBuildForCamp with DELTA encoding. The snapshot is keyed by TCP CONNECTION (not UObject*):
//! each connected peer carries its own ClientSnap. Emits IS1| (FULL) or IS2| (DELTA, only changed items).
static void srvBuildReply(ClientSnap* snap, UObject* camp, std::wstring& out) {
    std::wstring full; srvBuildForCamp(camp, full);   // IS1|name:cnt,... (guarded by SEH inside)
    if (!g_chDelta || snap->wantFull) { out = full; snap->wantFull = false; snap->lastFullMs = GetTickCount64(); snap->initialized = true; return; }
    //! parse IS1| into a sorted map for diffing
    std::map<std::wstring, int32_t> pool;
    size_t i = 4;   // skip "IS1|"
    while (i < full.size()) {
        size_t comma = full.find(L',', i);
        if (comma == std::wstring::npos) break;
        std::wstring tok = full.substr(i, comma - i);
        i = comma + 1;
        if (tok.empty()) continue;
        size_t colon = tok.rfind(L':');
        if (colon == std::wstring::npos) continue;
        int32_t cnt = 0; try { cnt = std::stoi(tok.substr(colon + 1)); } catch (...) {}
        if (cnt > 0) pool[tok.substr(0, colon)] = cnt;
    }
    uint64_t now = GetTickCount64();
    bool sendFull = !snap->initialized || (now - snap->lastFullMs) > g_chFullSyncMs;
    if (sendFull) {
        out = full;
        snap->lastPool = pool;
        snap->lastFullMs = now;
        snap->initialized = true;
        if (g_verbose) Output::send(STR("[ISGATE] L2: FULL sync len={}\n"), (int)out.size());
    } else {
        out = CH_DELTA_TAG;
        int changed = 0;
        for (auto& p : pool) {
            auto it = snap->lastPool.find(p.first);
            int32_t old = (it != snap->lastPool.end()) ? it->second : 0;
            if (p.second != old) { out += p.first + L":" + std::to_wstring(p.second) + L","; ++changed; }
        }
        for (auto& sp : snap->lastPool)
            if (pool.find(sp.first) == pool.end() && sp.second > 0) { out += sp.first + L":0,"; ++changed; }
        snap->lastPool = pool;
        if (g_verbose) Output::send(STR("[ISGATE] L2: DELTA sync changed={} len={}\n"), changed, (int)out.size());
    }
}
//! Pool-reply parser, shared by the TCP client receiver (extracted from the old hkChReply so the same
//! parsing/pathology-bookkeeping logic is reused). Parses IS1| (FULL replace) or IS2| (DELTA apply) into
//! g_pool; flags g_poolDirty (re-mint next detour) + g_lastFetchOk (self-heal) + clears in-flight/miss state.
static void parsePoolReply(const std::wstring& str) {
    bool isFull  = str.rfind(CH_SENTINEL, 0)  == 0;
    bool isDelta = str.rfind(CH_DELTA_TAG, 0) == 0;
    if (!isFull && !isDelta) return;
    g_awaitingReply = false;
    if (g_consecMiss > 0 && g_missLogged > 0) Output::send(STR("[ISGATE] CH channel recovered after {} consecutive misses\n"), g_consecMiss);
    g_consecMiss = 0; g_missLogged = 0;
    size_t i = 4;   // skip "IS1|" / "IS2|"
    if (isFull) {
        std::vector<std::pair<FName, int32_t>> np;
        while (i < str.size()) {
            size_t comma = str.find(L',', i);
            std::wstring tok = str.substr(i, comma == std::wstring::npos ? std::wstring::npos : comma - i);
            i = (comma == std::wstring::npos) ? str.size() : comma + 1;
            if (tok.empty()) continue;
            size_t colon = tok.rfind(L':'); if (colon == std::wstring::npos) continue;
            std::wstring nm = tok.substr(0, colon);
            long cnt = 0; try { cnt = std::stol(tok.substr(colon + 1)); } catch (...) { cnt = 0; }
            if (nm.empty() || cnt <= 0) continue;
            np.emplace_back(FName(nm.c_str()), (int32_t)cnt);
        }
        g_pool = std::move(np);
    } else {
        while (i < str.size()) {
            size_t comma = str.find(L',', i);
            std::wstring tok = str.substr(i, comma == std::wstring::npos ? std::wstring::npos : comma - i);
            i = (comma == std::wstring::npos) ? str.size() : comma + 1;
            if (tok.empty()) continue;
            size_t colon = tok.rfind(L':'); if (colon == std::wstring::npos) continue;
            std::wstring nm = tok.substr(0, colon);
            long cnt = 0; try { cnt = std::stol(tok.substr(colon + 1)); } catch (...) { cnt = 0; }
            if (nm.empty()) continue;
            FName id(nm.c_str());
            if (cnt <= 0) { for (auto it = g_pool.begin(); it != g_pool.end(); ++it) if (it->first == id) { g_pool.erase(it); break; } }
            else { bool f = false; for (auto& p : g_pool) if (p.first == id) { p.second = (int32_t)cnt; f = true; break; } if (!f) g_pool.emplace_back(id, (int32_t)cnt); }
        }
    }
    g_poolDirty = true;
    g_lastFetchOk = GetTickCount64();
    if (g_verbose) Output::send(STR("[ISGATE] CH-RECV {} len={} pool={} Wood={}\n"), isFull ? L"FULL" : L"DELTA", (int)str.size(), (int)g_pool.size(), poolGet(FName(STR("Wood"))));
}

// ============================================================================
//  EXTERNAL TCP TRANSPORT CHANNEL (replaces the Debug_CheatCommand RPC carrier)
// ============================================================================
//! One socket + ONE background thread, fully outside the UE net driver. The GAME THREAD does every
//! UObject read (srvBuildForCamp/srvCampById inside on_update); the NET THREAD does only send/recv and
//! shuttles whole lines between locked slots. Framing = one message per '\n'-terminated line.
//!   authority   -> listens on g_extPort; each accepted connection is a NetPeer carrying its own ClientSnap
//!   remote client -> connects to g_extHost:g_extPort (loopback for the host's own client)
//!   host/SP    -> reads cross-registered containers natively; needs NO channel
//! ASCII-only payloads (item ids + counts + hex guids) so byte/wchar truncation is safe.

//! ASCII codec helpers (item ids / hex guids are 7-bit ASCII).
static std::string wideToNarrow(const std::wstring& w) { std::string s; s.reserve(w.size()); for (wchar_t c : w) s.push_back((char)(c & 0x7F)); return s; }
static std::wstring narrowToWide(const std::string& s) { return std::wstring(s.begin(), s.end()); }
//! send a whole buffer; returns false on error (caller closes the socket).
static bool sockSendAll(SOCKET s, const std::string& msg) {
    const char* p = msg.data(); int left = (int)msg.size();
    while (left > 0) { int n = ::send(s, p, left, 0); if (n == SOCKET_ERROR) return false; p += n; left -= n; }
    return true;
}

//! One connected remote client on the authority. All fields are touched under g_netMu except rbuf (net
//! thread only) and dead (net thread only — set on recv error, consumed in the net thread's cleanup).
struct NetPeer {
    SOCKET sock = INVALID_SOCKET;
    std::string rbuf;               // net-thread recv line buffer
    std::wstring reqCampHex;        // net thread -> game thread (client's current camp GUID)
    bool hasReq = false;
    std::wstring reply;             // game thread -> net thread (built payload)
    bool hasReply = false;
    bool dead = false;              // net thread marks; removed under lock
    ClientSnap snap;                // L2 delta snapshot, per-connection
    std::wstring lastCampHex;       // v4.0.3: last requested camp - a change forces FULL re-sync (srvBuildForCamp's baseline differs per camp)
};

static std::mutex      g_netMu;                 // guards g_peers / g_listenSock / client slots
static std::atomic<bool> g_netRun{false};
static std::thread     g_netThread;
static bool            g_netStarted = false;    // netStart() called once after role is known
static std::atomic<bool> g_cliForceReconnect{false};  // v4.0.6: world-change reset asks the client net thread to reconnect so the server re-sends a FULL (delta snapshot is per-connection)
static SOCKET          g_listenSock = INVALID_SOCKET;
static std::vector<NetPeer*> g_peers;           // authority only; owned by the net thread
static SOCKET          g_cliSock = INVALID_SOCKET;   // remote-client only
static std::string     g_cliRbuf;               // net-thread client recv line buffer
static std::wstring    g_cliReqHex;             // game thread -> net thread
static bool            g_cliHasReq = false;
static std::wstring    g_cliReply;              // net thread -> game thread
static bool            g_cliHasReply = false;

//! Authority net thread: single-threaded select loop over the listen socket + every peer. accept() adds a
//! peer; recv feeds the per-peer line buffer (ISREQ|hex -> reqCampHex); a writable peer with a pending
//! reply gets it flushed. Dead peers are reaped under lock (the only place peers are freed, so the game
//! thread's locked traversal never sees a dangling pointer).
static void netServerThread() {
    while (g_netRun.load()) {
        fd_set rfds, wfds; FD_ZERO(&rfds); FD_ZERO(&wfds);
        FD_SET(g_listenSock, &rfds);
        std::vector<NetPeer*> snap;
        { std::lock_guard<std::mutex> lk(g_netMu);
          for (auto* p : g_peers) { if (p->dead) continue; snap.push_back(p); FD_SET(p->sock, &rfds);
              if (p->hasReply) FD_SET(p->sock, &wfds); } }
        timeval tv{0, 200000};   // 200ms
        int r = select(0, &rfds, &wfds, nullptr, &tv);
        (void)r;
        if (FD_ISSET(g_listenSock, &rfds)) {
            SOCKET cs = accept(g_listenSock, nullptr, nullptr);
            if (cs != INVALID_SOCKET) { u_long nb = 1; ioctlsocket(cs, FIONBIO, &nb);
                auto* p = new NetPeer(); p->sock = cs;
                std::lock_guard<std::mutex> lk(g_netMu); g_peers.push_back(p);
                if (g_verbose) Output::send(STR("[ISGATE] TCP peer connected (total={})\n"), (int)g_peers.size()); }
        }
        for (auto* p : snap) {
            if (p->dead) continue;
            if (FD_ISSET(p->sock, &rfds)) {
                char tmp[4096]; int n = recv(p->sock, tmp, sizeof(tmp), 0);
                if (n <= 0) { p->dead = true; continue; }
                p->rbuf.append(tmp, (size_t)n);
                size_t nl;
                while ((nl = p->rbuf.find('\n')) != std::string::npos) {
                    std::string line = p->rbuf.substr(0, nl); p->rbuf.erase(0, nl + 1);
                    if (line.rfind("ISREQ|", 0) == 0) { std::lock_guard<std::mutex> lk(g_netMu); p->reqCampHex = narrowToWide(line.substr(6)); p->hasReq = true; }
                }
            }
            if (FD_ISSET(p->sock, &wfds)) {
                std::string msg;
                { std::lock_guard<std::mutex> lk(g_netMu); if (p->hasReply) { msg = wideToNarrow(p->reply); p->reply.clear(); p->hasReply = false; } }
                if (!msg.empty()) { msg.push_back('\n'); if (!sockSendAll(p->sock, msg)) p->dead = true; }
            }
        }
        { std::lock_guard<std::mutex> lk(g_netMu);
          for (auto it = g_peers.begin(); it != g_peers.end(); ) {
              if ((*it)->dead) { if ((*it)->sock != INVALID_SOCKET) closesocket((*it)->sock); delete *it; it = g_peers.erase(it); }
              else ++it; } }
    }
}

//! Remote-client net thread: blocking socket + 1s recv/send timeouts for liveness. Connects to
//! g_extHost:g_extPort, reconnects with a 3s backoff on any failure. Pumps pending requests out and
//! reply lines in. Runs on its own thread so a stalled connect never blocks the game thread.
static void netClientThread() {
    while (g_netRun.load()) {
        std::string hostN = wideToNarrow(g_extHost);
        std::string portN = std::to_string((unsigned)g_extPort);
        addrinfo hints{}; hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM; hints.ai_protocol = IPPROTO_TCP;
        addrinfo* res = nullptr; SOCKET s = INVALID_SOCKET;
        if (getaddrinfo(hostN.c_str(), portN.c_str(), &hints, &res) == 0) {
            for (auto* a = res; a && s == INVALID_SOCKET; a = a->ai_next) {
                s = socket(a->ai_family, a->ai_socktype, a->ai_protocol);
                if (s != INVALID_SOCKET) { if (connect(s, a->ai_addr, (int)a->ai_addrlen) != 0) { closesocket(s); s = INVALID_SOCKET; } }
            }
            freeaddrinfo(res);
        }
        if (s == INVALID_SOCKET) { for (int i = 0; i < 30 && g_netRun.load(); ++i) Sleep(100); continue; }
        DWORD tmo = 1000; setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (char*)&tmo, sizeof(tmo));
        setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, (char*)&tmo, sizeof(tmo));
        g_cliRbuf.clear();
        { std::lock_guard<std::mutex> lk(g_netMu); g_cliSock = s; }
        if (g_verbose) Output::send(STR("[ISGATE] TCP client connected to {}:{}\n"), g_extHost.c_str(), (unsigned)g_extPort);
        while (g_netRun.load()) {
            //! v4.0.6: a world-change reset cleared g_pool, but the server's delta snapshot is per-connection
            //! and still thinks it sent the full pool -> it would emit a tiny DELTA that applied to an empty
            //! pool yields a broken pool (pool=4, Wood=-1 after title->rejoin). Reconnect so the server treats
            //! us as a new peer (fresh snapshot -> FULL).
            if (g_cliForceReconnect.load()) { g_cliForceReconnect.store(false); break; }
            std::string outReq;
            { std::lock_guard<std::mutex> lk(g_netMu); if (g_cliHasReq) { outReq = "ISREQ|" + wideToNarrow(g_cliReqHex); g_cliHasReq = false; } }
            if (!outReq.empty()) { outReq.push_back('\n'); if (!sockSendAll(s, outReq)) break; }
            char tmp[4096]; int n = recv(s, tmp, sizeof(tmp), 0);
            if (n == 0) break;
            if (n < 0) { if (WSAGetLastError() == WSAETIMEDOUT) continue; break; }
            g_cliRbuf.append(tmp, (size_t)n);
            size_t nl;
            while ((nl = g_cliRbuf.find('\n')) != std::string::npos) {
                std::string line = g_cliRbuf.substr(0, nl); g_cliRbuf.erase(0, nl + 1);
                std::wstring w = narrowToWide(line);
                if (w.rfind(CH_SENTINEL, 0) == 0 || w.rfind(CH_DELTA_TAG, 0) == 0) { std::lock_guard<std::mutex> lk(g_netMu); g_cliReply = w; g_cliHasReply = true; }
            }
        }
        { std::lock_guard<std::mutex> lk(g_netMu); if (g_cliSock == s) g_cliSock = INVALID_SOCKET; }
        closesocket(s);
        if (g_verbose && g_netRun.load()) Output::send(STR("[ISGATE] TCP client disconnected -> reconnect in 3s\n"));
        for (int i = 0; i < 30 && g_netRun.load(); ++i) Sleep(100);
    }
}

//! Bring up the channel for THIS process's role. Called once from on_update after the role is known
//! (the role isn't available at on_unreal_init — it needs an in-game PalPlayerCharacter).
static void netStart() {
    if (!g_extEnabled) { Output::send(STR("[ISGATE] TCP: disabled by config\n")); return; }
    WSADATA wsa; if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) { Output::send(STR("[ISGATE] TCP: WSAStartup failed\n")); return; }
    g_netRun.store(true);
    if (g_isSrv == 1) {   // authority listens
        g_listenSock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (g_listenSock == INVALID_SOCKET) { Output::send(STR("[ISGATE] TCP: listener socket failed\n")); }
        else {
            BOOL reuse = TRUE; setsockopt(g_listenSock, SOL_SOCKET, SO_REUSEADDR, (char*)&reuse, sizeof(reuse));
            sockaddr_in sa{}; sa.sin_family = AF_INET; sa.sin_addr.s_addr = htonl(INADDR_ANY); sa.sin_port = htons(g_extPort);
            if (bind(g_listenSock, (sockaddr*)&sa, sizeof(sa)) == SOCKET_ERROR || listen(g_listenSock, 8) == SOCKET_ERROR) {
                Output::send(STR("[ISGATE] TCP: bind/listen port {} failed (err={})\n"), (unsigned)g_extPort, WSAGetLastError());
                closesocket(g_listenSock); g_listenSock = INVALID_SOCKET;
            } else {
                u_long nb = 1; ioctlsocket(g_listenSock, FIONBIO, &nb);
                Output::send(STR("[ISGATE] TCP server listening on port {}\n"), (unsigned)g_extPort);
            }
        }
        if (g_listenSock != INVALID_SOCKET) g_netThread = std::thread(netServerThread);
    } else if (g_isSrv == 0 && !g_extHost.empty()) {   // remote client connects
        g_netThread = std::thread(netClientThread);
        Output::send(STR("[ISGATE] TCP client -> {}:{}\n"), g_extHost.c_str(), (unsigned)g_extPort);
    } else {
        Output::send(STR("[ISGATE] TCP: no channel for this role (host/SP reads containers natively)\n"));
    }
}

//! Tear down the channel (mod unload / hot-reload). Joins the net thread so no socket op outlives the DLL.
static void netStop() {
    g_netRun.store(false);
    if (g_netThread.joinable()) g_netThread.join();
    std::lock_guard<std::mutex> lk(g_netMu);
    for (auto* p : g_peers) { if (p->sock != INVALID_SOCKET) closesocket(p->sock); delete p; }
    g_peers.clear();
    if (g_cliSock != INVALID_SOCKET) { closesocket(g_cliSock); g_cliSock = INVALID_SOCKET; }
    if (g_listenSock != INVALID_SOCKET) { closesocket(g_listenSock); g_listenSock = INVALID_SOCKET; }
    g_cliHasReq = false; g_cliHasReply = false;
    WSACleanup();
}
//! (The old Debug_ReceiveCheatCommand_ToClient RPC hook is gone — replies now arrive over the external TCP
//! channel and are parsed by parsePoolReply above. The game thread consumes g_cliReply in on_update.)
//! PURE-READ local in-camp test (factored from chClientTrigger's gate). Resolves the local pawn via the
//! PlayerController's K2_GetPawn, then reads NowInsideBaseCampID's HIGH 8 bytes off the check component
//! (@0xC08 -> @0xC0). "Inside" == first 8 bytes non-zero (the field isn't zeroed on exit; a real camp guid
//! has a random non-zero high half). NO reflection on the check component (that AV'd out of camp as dev-3.2).
//! THROTTLED: the probe (FindFirstOf(PalPlayerController) = O(all UObjects) + a K2_GetPawn ProcessEvent) is
//! FAR too heavy to run per collect call — in-camp with a pool, injectMinted fires every frame, so an
//! unthrottled probe here tanks the frame rate during the frequent-collect phase. Camp enter/exit is not a
//! per-frame event, so cache the result and only re-probe every ~300ms; every other call is a tick compare.
static bool clientInCamp() {
    static uint64_t s_last = 0;
    static bool     s_cached = false;
    uint64_t now = GetTickCount64();
    if (s_last != 0 && now - s_last < 500) return s_cached;     // serve cache between probes (C2: 300->500ms; enter/exit isn't per-frame)
    s_last = now;
    __try {
        UObject* ctrl = UObjectGlobals::FindFirstOf(STR("PalPlayerController")); if (!ctrl) return (s_cached = false);
        UFunction* getPawn = ctrl->GetFunctionByNameInChain(STR("K2_GetPawn")); if (!getPawn) return (s_cached = false);
        struct { UObject* Ret; } pp{}; ctrl->ProcessEvent(getPawn, &pp);
        UObject* pawn = pp.Ret; if (!pawn) return (s_cached = false);
        UObject* chk = *(UObject**)((uint8_t*)pawn + OFF_PAWN_CAMPCHECK); if (!chk) return (s_cached = false);
        //! v4.0.5: use the native IsInsideBaseCamp() predicate (returns bool) instead of reading
        //! NowInsideBaseCampID's high 8 bytes. The GUID field is cleared by the game while idle in-camp,
        //! which falsely read "outside" and starved the pool for minutes. IsInsideBaseCamp() is the
        //! engine's own stable in-camp test. SEH-guarded by the surrounding __try.
        UFunction* isInside = chk->GetFunctionByNameInChain(STR("IsInsideBaseCamp"));
        if (isInside) {
            struct { bool Ret; uint8_t pad[7]; } ir{}; chk->ProcessEvent(isInside, &ir);
            return (s_cached = ir.Ret);
        }
        //! fallback to the GUID read if the function isn't resolved (older build)
        const uint8_t* campGuid = (const uint8_t*)chk + OFF_CHK_CAMPID;
        uint32_t gHiA = *(const uint32_t*)(campGuid + 0), gHiB = *(const uint32_t*)(campGuid + 4);
        return (s_cached = !(gHiA == 0 && gHiB == 0));
    } __except (EXCEPTION_EXECUTE_HANDLER) { return (s_cached = false); }
}
//! A1 — DEBOUNCED "confirmed in camp". The raw NowInsideBaseCampID field can transiently read a zero high half
//! during camp-boundary recalc / component updates, so a single clientInCamp() miss must NOT clear the pool.
//! We require ~4 NEW samples (>=300ms apart) that all disagree with the cached state before flipping it, so a
//! transient glitch is absorbed while a genuine enter/exit still settles within ~1-2s. Single source of truth
//! for the display out-of-camp gate (injectMinted) and the trigger retry/stop decision (chClientTrigger).
static bool clientInCampStable() {
    uint64_t now = GetTickCount64();
    bool raw = clientInCamp();
    if (now - g_inCampLastSampleAt >= 300) {            // advance the streak on a genuinely new sample only
        g_inCampLastSampleAt = now;
        if (raw == g_inCampStable) g_inCampStreak = 0;
        else if (++g_inCampStreak >= 4) { g_inCampStable = raw; g_inCampStreak = 0; }
    }
    //! v4.0.2: the hook-driven state is AUTHORITATIVE. The raw NowInsideBaseCampID poll reads a zero high
    //! half while idle in-camp (the field is transiently cleared by the game), which used to flip
    //! g_inCampStable=false and starve the pool for minutes ("AFK in camp -> mod dead"). Enter/exit fire on
    //! real boundary crossings, so trust them once seen; fall back to the debounced poll only before the
    //! first hook event (title screen / very first frame).
    //! v4.0.4: OR the hook with the debounced poll. A spurious exit hook must NOT alone clear inCamp while
    //! the poll still reads inside (that dropped the pool). Either signal "in" is enough; the enter hook
    //! keeps it sticky through AFK (the poll clears NowInsideBaseCampID while idle).
    if (g_inCampHookKnown) return g_inCampHook || g_inCampStable;
    return g_inCampStable;
}
//! client: read the LOCAL camp GUID off the pawn's InsideBaseCampCheckComponent and return it (outHex).
//! Returns true ONLY when the player is confirmed inside a camp with a valid GUID. Does NOT send anything —
//! the caller (on_update) queues "ISREQ|"+hex onto the external TCP channel. Everything resolved live (no
//! cached ptr -> no dangling crash). ONLY call from on_update (top-level tick).
static bool chClientTriggerInner(std::wstring& outHex) {
    UObject* ctrl = UObjectGlobals::FindFirstOf(STR("PalPlayerController")); if (!ctrl) return false;
    UFunction* getPawn = ctrl->GetFunctionByNameInChain(STR("K2_GetPawn")); if (!getPawn) return false;
    struct { UObject* Ret; } pp{}; ctrl->ProcessEvent(getPawn, &pp);
    UObject* pawn = pp.Ret; if (!pawn) return false;
    UObject* chk = *(UObject**)((uint8_t*)pawn + OFF_PAWN_CAMPCHECK);
    if (!chk) { if (g_verbose) Output::send(STR("[ISGATE] CH skip: no InsideBaseCampCheckComponent poolNow={}\n"), (int)g_pool.size()); return false; }
    uint8_t* campGuid = (uint8_t*)chk + OFF_CHK_CAMPID;
    //! AUTHORITATIVE in-camp gate — PURE READ, no reflection on `chk`. The raw NowInsideBaseCampID field
    //! @0xC0 is NOT zeroed when the player leaves a camp: the pawn's camp-check slot then reads back a stale
    //! PARTIAL guid whose FIRST 8 bytes are zero (e.g. 0000000000000000f010c5bc2902....), so guidZero()
    //! (all-16-zero) alone lets a bogus out-of-camp request through -> the server logs "camp not found".
    //! A real camp FGuid has a random non-zero high half (dwords A/B), so "inside" == first 8 bytes present.
    //! Do NOT call GetFunctionByNameInChain/ProcessEvent on `chk` here: out of camp that slot can point at a
    //! stale/other object — reading its flat bytes is harmless (historically safe), but walking it as a
    //! UObject via reflection follows garbage internal pointers and access-violates (this crashed as dev-3.2:
    //! on_update -> chClientTrigger -> chk->GetFunctionByNameInChain -> UE4SS `mov rcx,[rax]`, rax=0x400000049).
    uint32_t gHiA = *(const uint32_t*)(campGuid + 0);
    uint32_t gHiB = *(const uint32_t*)(campGuid + 4);
    if (gHiA == 0 && gHiB == 0) {                                // GUID not valid right now (entering / not yet replicated /
                                                                  // a transient zero in the raw field). The destructive pool-clear
                                                                  // that used to live here was the #1 cause of "materials suddenly
                                                                  // show only the current camp": a single transient zero wiped g_pool
                                                                  // AND consumed the trigger, with no auto-recovery. Now the pool is
                                                                  // owned by injectMinted's DEBOUNCED out-of-camp gate (clientInCampStable);
                                                                  // here we only stop retrying once CONFIRMED out of camp, and otherwise
                                                                  // keep the trigger so the next on_update tick retries with a valid GUID.
        if (!clientInCampStable()) g_needTrigger = false;        // confirmed out of camp -> stop retrying
        return false;                                            // transient / entering -> keep trigger, retry
    }
    wchar_t hex[33]; hexOf(campGuid, hex);
    outHex = hex;                              // caller (on_update) queues "ISREQ|"+hex onto the TCP channel
    ++g_myCalls;
    if (g_verbose) Output::send(STR("[ISGATE] CH request camp={} myCalls={}\n"), hex, (unsigned)g_myCalls);
    return true;
}
static bool chClientTrigger(std::wstring& outHex) {
    __try { return chClientTriggerInner(outHex); }
    __except (EXCEPTION_EXECUTE_HANDLER) {
        if (g_errLog < 64) { ++g_errLog; Output::send(STR("[ISGATE] chClientTrigger: AV guarded (stale UObject) -> skipping this tick\n")); }
        return false;
    }
}
//! client: EVENT-DRIVEN camp tracking (no polling). OnEnterBaseCamp fires when the local player enters a
//! camp -> drop the old pool and flag a fresh request (fired from on_update). The reply arrives before a
//! build menu opens, so there's no round-trip latency at collect time. OnExitBaseCamp is intentionally a
//! no-op: it fires spuriously (enter+exit paired, and while standing still), and clearing the pool there
//! dropped the display to 0 mid-build. The pool is refreshed on the next enter + on every menu-open edge.
static void hkEnterCamp(UnrealScriptFunctionCallableContext& ctx, void*) {
    if (!isClient()) return;
    //! OnEnterBaseCamp is a NetMulticast: the server fires it on the entering player's PalBuilderComponent and
    //! replicates it to ALL clients. Without filtering, every remote player's camp-entry event clears OUR pool
    //! and triggers a redundant server request — in a 3-player session this turned into a constant RPC storm
    //! that saturated the reliable channel and stalled ALL game interactions (pal summon, eat, build, etc).
    //! Filter: only react when the PalBuilderComponent that fired belongs to the LOCAL player.
    if (ctx.Context) {
        UObject* eventOwner = ctx.Context->GetOuterPrivate();   // PalBuilderComponent -> owning character
        __try {
            UObject* ctrl = UObjectGlobals::FindFirstOf(STR("PalPlayerController"));
            if (ctrl) {
                UFunction* gp = ctrl->GetFunctionByNameInChain(STR("K2_GetPawn"));
                if (gp) { struct { UObject* Ret; } pp{}; ctrl->ProcessEvent(gp, &pp); if (pp.Ret && pp.Ret != eventOwner) return; }
            }
        } __except (EXCEPTION_EXECUTE_HANDLER) {}
    }
    g_poolDirty = true; g_needTrigger = true;   //! v4.0.7: do NOT clear g_pool. Same-camp re-entry (walk out then in) emptied g_pool while the server sent a DELTA (snapshot unchanged) -> pool truncated (pool=5/Wood=-1). Server FULL (camp change) / DELTA refresh the pool correctly without us clearing it.
    g_inCampHook = true; g_inCampHookKnown = true;   //! v4.0.2: authoritative in-camp signal (survives AFK, NowInsideBaseCampID clears while idle)
    g_lastEnterAt = GetTickCount64();                //! v4.0.4: mark enter time so a same-instant exit (boundary multicast pair) is suppressed
    if (g_verbose) Output::send(STR("[ISGATE] CH enter-camp -> flagged (inCamp=true)\n"));
}
static void hkExitCamp(UnrealScriptFunctionCallableContext& ctx, void*) {
    if (!isClient()) return;
    //! Same local-player filter as hkEnterCamp (OnExitBaseCamp is also a NetMulticast; without it every
    //! remote player's exit would flip OUR state).
    if (ctx.Context) {
        UObject* eventOwner = ctx.Context->GetOuterPrivate();
        __try {
            UObject* ctrl = UObjectGlobals::FindFirstOf(STR("PalPlayerController"));
            if (ctrl) {
                UFunction* gp = ctrl->GetFunctionByNameInChain(STR("K2_GetPawn"));
                if (gp) { struct { UObject* Ret; } pp{}; ctrl->ProcessEvent(gp, &pp); if (pp.Ret && pp.Ret != eventOwner) return; }
            }
        } __except (EXCEPTION_EXECUTE_HANDLER) {}
    }
    //! v4.0.4: suppress an exit firing right after an enter. OnEnterBaseCamp + OnExitBaseCamp arrive as a
    //! multicast pair within ~20ms on a boundary crossing; the exit flipped inCamp=false -> injectMinted
    //! dropped the pool ("out of camp -> dropped stale pool") while the player was actually in camp. Ignore
    //! an exit within 3s of the last enter; a genuine exit has no recent enter.
    if (g_lastEnterAt != 0 && (GetTickCount64() - g_lastEnterAt) < 3000) return;
    g_inCampHook = false; g_inCampHookKnown = true;   //! v4.0.2: authoritative out-of-camp signal
    if (g_verbose) Output::send(STR("[ISGATE] CH exit-camp -> inCamp=false\n"));
}
//! client: most overlay menus open through the central dispatcher UPalUserWidget:Push -> covers build, the
//! ESC/pause menu, and others. We don't care WHICH menu; just flag a fresh pool request. The back-pressure
//! guard (in-camp + CH_MIN_INTERVAL_MS + in-flight lock) keeps this cheap even though Push fires for all
//! overlays. on_update does the actual send from a safe context.
static void hkPush(UnrealScriptFunctionCallableContext& ctx, void*) {
    (void)ctx;
    if (!isClient()) return;
    g_needTrigger = true;
    if (g_verbose) Output::send(STR("[ISGATE] CH menu-open (Push) -> flagged\n"));
}
//! client: the CRAFT/production menu does NOT route through UPalUserWidget:Push (confirmed live: build + ESC
//! fire Push, craft does not). Its UI model binds via UPalUIConvertItemModel:Initialize -> hook that as a
//! second menu-open edge so opening a workbench/production facility refreshes the pool too. Same flag-only
//! body; we never touch the param.
static void hkCraftOpen(UnrealScriptFunctionCallableContext& ctx, void*) {
    (void)ctx;
    if (!isClient()) return;
    g_needTrigger = true;
    if (g_verbose) Output::send(STR("[ISGATE] CH menu-open (Craft) -> flagged\n"));
}
//! (Layer 1 — make the reply RPC unreliable — is GONE: the reply no longer travels over a game RPC at all,
//! it travels over the external TCP channel, so there is no reliable buffer to saturate.)


static void installChannel() {
    auto noop = [](UnrealScriptFunctionCallableContext&, void*) {};
    //! The pool data now travels over the external TCP channel (netStart), NOT over the Debug_CheatCommand
    //! RPC pair. Only the display-side EVENT hooks remain here — they just flag g_needTrigger (a bool the
    //! game thread consumes in on_update to fire a TCP request). enter/exit + the two menu-open edges.
    try {
        UObjectGlobals::RegisterHook(STR("/Script/Pal.PalBuilderComponent:OnEnterBaseCamp"), noop, hkEnterCamp, nullptr);
        UObjectGlobals::RegisterHook(STR("/Script/Pal.PalBuilderComponent:OnExitBaseCamp"),  noop, hkExitCamp,  nullptr);
        Output::send(STR("[ISGATE] hooks OK: OnEnter/OnExitBaseCamp (camp tracking)\n"));
    } catch (const std::exception&) { Output::send(STR("[ISGATE] hook FAILED: OnEnter/OnExitBaseCamp\n")); }
    //! Single universal menu-open edge: the central overlay dispatcher. Own try so a bad path can't take the
    //! enter/exit hooks down, and the log confirms it resolved.
    try {
        UObjectGlobals::RegisterHook(STR("/Script/Pal.PalUserWidget:Push"), noop, hkPush, nullptr);
        Output::send(STR("[ISGATE] hook OK: /Script/Pal.PalUserWidget:Push (menu-open: build/esc/etc)\n"));
    } catch (const std::exception&) { Output::send(STR("[ISGATE] hook FAILED: /Script/Pal.PalUserWidget:Push\n")); }
    //! Craft/production menu-open (does NOT go through Push). Own try so a bad path can't take Push down.
    try {
        UObjectGlobals::RegisterHook(STR("/Script/Pal.PalUIConvertItemModel:Initialize"), noop, hkCraftOpen, nullptr);
        Output::send(STR("[ISGATE] hook OK: /Script/Pal.PalUIConvertItemModel:Initialize (craft menu-open)\n"));
    } catch (const std::exception&) { Output::send(STR("[ISGATE] hook FAILED: /Script/Pal.PalUIConvertItemModel:Initialize\n")); }
}

// ============================================================================
//  Lifecycle — drop cached world-object state on a world change
// ============================================================================
//! Every UObject* we cache belongs to the CURRENT world and is freed when it unloads; without this reset,
//! re-entering a save reuses dangling pointers -> crash. CDOs/UClasses (g_palUtil, g_itemUtilCdo,
//! g_createSlotFn) are /Script objects, not world objects -> kept. Role is reset so it recomputes from the
//! new in-game world.
static UObject* g_lastWorld = nullptr;
static void resetState() {
    g_guilds.clear(); g_instToCamp.clear(); g_instToCont.clear(); g_campIdToCamp.clear(); g_registered.clear();
    for (UObject* s : g_mintedSlots) if (s) s->ClearRootSet();   // unroot so the old world's minted slots can be GC'd
    g_mintedSlots.clear();
    g_common = nullptr; g_donorCont = nullptr;
    g_pool.clear();
    g_cliForceReconnect.store(true);   //! v4.0.6: pool wiped -> force TCP reconnect so the server re-sends FULL (delta snapshot is per-connection; a stale DELTA on an empty pool corrupts it)
    g_swapped2 = false; g_swapDonor = nullptr; g_swapBuf.clear();   // drop the append buffer + pinned donor of the old world
    g_mintStampDirty = true; g_lastStampRealNum = -1;               // C1: force a re-stamp in the new world
    g_srvInjecting = false; g_injectDepth = 0;
    g_poolDirty = false; g_needTrigger = false;
    g_awaitingReply = false; g_lastTrigAt = 0;
    g_consecMiss = 0; g_missLogged = 0;                               // channel health re-derived in new world
    g_lastFetchOk = 0;                                              // A4: no fetch in the new world yet
    g_inCampStable = false; g_inCampStreak = 0; g_inCampLastSampleAt = 0;   // A1: re-derive in-camp state in the new world
    g_inCampHook = false; g_inCampHookKnown = false;   //! v4.0.2: re-derive hook-driven in-camp in the new world
    g_isSrv = -1; g_lastWc = nullptr;   //! v4.0.1: keep g_isDedi — dedicated is process-fixed, survives a world change (so ensureRole's fault-fallback can still authorize after a reset)
    g_errLog = 0;                                                    // fresh per-world budget for always-on error diagnostics
    g_roleFalseCount = 0;                                            // clear role-watchdog streak
    //! TCP channel: the connection survives a world change, but every peer's delta snapshot must restart
    //! (force a FULL re-sync) and any stale client-side reply is dropped. New camp GUIDs re-fetch on demand.
    { std::lock_guard<std::mutex> lk(g_netMu);
      for (auto* p : g_peers) { p->snap.wantFull = true; p->snap.initialized = false; p->hasReq = false; p->hasReply = false; }
      g_cliHasReply = false; g_cliHasReq = false; }
    Output::send(STR("[ISGATE] world change -> full state reset\n"));
}
static void checkWorld(void* anyObj) {
    __try {
        if (!anyObj) return;
        UObject* w = ((UObject*)anyObj)->GetWorld();
        if (!w) return;
        if (w != g_lastWorld) { if (g_lastWorld) resetState(); g_lastWorld = w; }
    } __except (EXCEPTION_EXECUTE_HANDLER) {}
}

// ============================================================================
//  config.txt loader
// ============================================================================
//! Locate our own DLL (address-of a local fn -> module handle -> path), walk up two dirs
//! (.../Mods/<Name>/dlls/main.dll -> .../Mods/<Name>), parse `key = value` lines (`#`/`;` start a comment).
//! Absent file / unknown keys are fine -> defaults hold. Keys: verbose (bool), reconcile_interval_ms (>=500),
//! isi_refresh_ms (>=200, reserved).
static void loadConfig() {
    HMODULE hm = nullptr;
    if (!GetModuleHandleExW(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                            (LPCWSTR)&loadConfig, &hm) || !hm) return;
    wchar_t buf[MAX_PATH]; DWORD n = GetModuleFileNameW(hm, buf, MAX_PATH);
    if (n == 0 || n >= MAX_PATH) return;
    std::wstring p(buf, n);
    for (int up = 0; up < 2; ++up) { size_t s = p.find_last_of(L"\\/"); if (s == std::wstring::npos) break; p.resize(s); }
    std::wifstream f(p + L"\\config.txt");
    if (!f) return;
    auto trim = [](std::wstring s) -> std::wstring {
        size_t a = s.find_first_not_of(L" \t\r\n"); if (a == std::wstring::npos) return L"";
        return s.substr(a, s.find_last_not_of(L" \t\r\n") - a + 1);
    };
    auto lower = [](std::wstring s) { for (auto& c : s) if (c >= L'A' && c <= L'Z') c = (wchar_t)(c + 32); return s; };
    std::wstring line;
    while (std::getline(f, line)) {
        size_t h = line.find_first_of(L"#;"); if (h != std::wstring::npos) line.resize(h);
        size_t eq = line.find(L'='); if (eq == std::wstring::npos) continue;
        std::wstring key = lower(trim(line.substr(0, eq)));
        std::wstring val = lower(trim(line.substr(eq + 1)));
        std::wstring rawVal = trim(line.substr(eq + 1));   // un-lowered (server host/IP may be case-sensitive)
        if (key.empty() || val.empty()) continue;
        auto asBool = [&](bool def) { return (val == L"1" || val == L"true" || val == L"yes" || val == L"on") ? true
                                           : (val == L"0" || val == L"false" || val == L"no" || val == L"off") ? false : def; };
        long num = 0; { try { num = std::stol(val); } catch (...) { num = -1; } }
        if      (key == L"verbose")                             g_verbose      = asBool(g_verbose);
        else if (key == L"reconcile_interval_ms" && num >= 500) g_reconcileMs  = (uint64_t)num;
        else if (key == L"isi_refresh_ms"        && num >= 200) g_isiRefreshMs = (uint64_t)num;
        else if (key == L"external_channel")                   g_extEnabled   = asBool(g_extEnabled);
        else if (key == L"external_port"          && num > 0 && num < 65536) g_extPort = (uint16_t)num;
        else if (key == L"external_server_port"   && num > 0 && num < 65536) g_extPort = (uint16_t)num;
        else if (key == L"external_server_host"   && !rawVal.empty())       g_extHost = rawVal;
        else if (key == L"channel_delta")                       g_chDelta      = asBool(g_chDelta);
        else if (key == L"channel_full_sync_interval" && num >= 5000) g_chFullSyncMs = (uint64_t)num;
    }
}

// ============================================================================
//  Mod entry
// ============================================================================
static constexpr bool EN_COLLECT = true, EN_CATALOG = true, EN_PLACEMENT = true;   // per-detour install toggles (diagnostics)

class ModIntegratedStorageCpp : public CppUserModBase
{
public:
    ModIntegratedStorageCpp() : CppUserModBase()
    {
        ModName = STR("IntegratedStorageCpp"); ModVersion = STR("4.1-probe-20260809");
        ModDescription = STR("Cross-camp build/craft: use any same-guild camp's stored materials at any camp. Server cross-registers guild containers; the remote client displays the guild total via a custom ISI-free transport channel. AOB-signature located (survives game updates).");
        ModAuthors = STR("Sarfflow");
    }
    ~ModIntegratedStorageCpp() override
    {
        netStop();   // join the net thread + close sockets BEFORE tearing down anything else
        for (Target* t : { &g_collect, &g_7d0, &g_ac0 })
            if (t->det) { if (t->hooked) t->det->unHook(); delete t->det; t->det = nullptr; }
        for (UObject* s : g_mintedSlots) if (s) s->ClearRootSet();   // release the rooted minted slots on unload/hot-reload (else they leak in the root set)
        g_mintedSlots.clear();
    }
    auto on_unreal_init() -> void override
    {
        loadConfig();
        const uintptr_t base = (uintptr_t)GetModuleHandleW(nullptr);
        Output::send(STR("[ISGATE] === IntegratedStorage {} loaded, base {:#x} ===\n"), ModVersion, base);
        initExecRanges(base);
        Output::send(STR("[ISGATE] exec sections={}\n"), (int)g_exec.size());
        auto maybe = [&](bool en, Target& t, uint64_t cb) {
            if (en) install(base, t, cb);
            else Output::send(STR("[ISGATE] {} OFF\n"), t.name);
        };
        //! CLIENT display detours (gated at runtime by isClient(); pure pass-through on the authority).
        maybe(EN_COLLECT,   g_collect, (uint64_t)&hkCollect);
        maybe(EN_CATALOG,   g_7d0,     (uint64_t)&hk7d0);
        maybe(EN_PLACEMENT, g_ac0,     (uint64_t)&hkAc0);
        //! Transport channel hooks (both ends; role-gated inside the handlers). Server consume is the ~8s
        //! reconcile in on_update. Native ISI is never touched.
        installChannel();
        Output::send(STR("[ISGATE] config: verbose={} reconcile_ms={} ext={} ext_port={} ext_host={} ch_delta={} ch_full_ms={}\n"),
            g_verbose, (int)g_reconcileMs, (int)g_extEnabled, (unsigned)g_extPort, g_extHost.c_str(), (int)g_chDelta, (int)g_chFullSyncMs);
    }
    auto on_update() -> void override {
        uint64_t now = GetTickCount64();
        //! ROLE-INDEPENDENT world-change probe (fixes SP->title->dedicated role caching). checkWorld() resets
        //! all cached world state INCLUDING g_isSrv when the UWorld pointer changes — but it was ONLY ever
        //! called from the CLIENT display detours, which short-circuit on the server role (`if(!isClient())
        //! return tramp`). So a process that started in single-player (server role) could never observe the
        //! world change to un-stick itself: it stayed "server" forever and never sent a CH request after
        //! joining a dedicated server. on_update runs for EVERY role, so probe here. FindFirstOf is
        //! O(all UObjects) -> throttle to ~1s (never per-frame); role transitions only happen on map load, so
        //! ~1s latency is invisible. checkWorld(null) at the title screen is a safe no-op.
        static uint64_t g_lastWorldProbe = 0;
        if (now - g_lastWorldProbe > 1000) {
            g_lastWorldProbe = now;
            checkWorld(UObjectGlobals::FindFirstOf(STR("PalPlayerCharacter")));
        }
        //! (b) ROLE WATCHDOG: role is cached once and only re-derived on a world POINTER change (checkWorld
        //! above). But role can also drift WITHOUT a world change (an SP->title->dedicated transition, or
//! IsServer flipping during a fragile replication window), and once g_isSrv is stuck wrong the client
//! drops EVERY reply (the isClient() guard) / the server never reconciles -> a PERMANENT "only
        //! current camp" that no menu-open or camp-enter can fix. So every ~10s, if a PalPlayerCharacter exists,
        //! re-read IsServer and compare; on disagreement reset everything so it re-derives. Cheap (one
        //! ProcessEvent / 10s) and only acts on a real drift. Title menu has no PalPlayerCharacter -> no probe.
        if (now - g_lastRoleCheck > 10000) {
            g_lastRoleCheck = now;
            UObject* pc = UObjectGlobals::FindFirstOf(STR("PalPlayerCharacter"));
            if (pc) {
                bool faulted = false;
                int fresh = callUtilBool(STR("IsServer"), pc, &faulted) ? 1 : 0;
                //! A FAULT here (transient AV while the first PalPlayerCharacter is being torn down / its world
                //! mid-replication) must NOT be treated as a real role change — it is indistinguishable from
                //! "IsServer=false" only if we ignore the fault flag, which is what flipped a dedicated server
                //! to CLIENT on 2026-08-02 (20:50:49: cached=1 fresh=0 -> full reset). Skip this tick instead;
                //! the next ~10s probe re-reads on a (likely) stable character.
                if (faulted) {
                    g_roleFalseCount = 0;
                    if (g_errLog < 64) { ++g_errLog; Output::send(STR("[ISGATE] ROLE watchdog: IsServer probe faulted (transient AV) -> skip\n")); }
                } else if (g_isSrv >= 0 && g_isSrv != fresh && !(g_isDedi == 1 && fresh == 0)) {   //! dedicated: IsServer=false is transient, skip
                    //! Require 3 CONSECUTIVE disagreements (~30s) before committing to a reset. A single
                    //! IsServer=false on a dedicated server is almost always a transient false reading caused
                    //! by FindFirstOf returning a PalPlayerCharacter in a replication/loading transition —
                    //! resetting on the first false positive flips the server to CLIENT and disconnects everyone.
                    ++g_roleFalseCount;
                    if (g_roleFalseCount >= 3) {
                        if (g_errLog < 64) { ++g_errLog; Output::send(STR("[ISGATE] ROLE watchdog: cached={} fresh={} sustained {}x -> full reset\n"), g_isSrv, fresh, g_roleFalseCount); }
                        g_roleFalseCount = 0;
                        resetState();
                    } else {
                        if (g_errLog < 64) { ++g_errLog; Output::send(STR("[ISGATE] ROLE watchdog: cached={} fresh={} ({} of 3) -> waiting for confirmation\n"), g_isSrv, fresh, g_roleFalseCount); }
                    }
                } else {
                    g_roleFalseCount = 0;   // agreement — reset streak
                }
            }
        }
        //! Resolve role first; do nothing until it's known (title menu has no PalPlayerCharacter).
        if (g_isSrv < 0) {
            isClient();
            //! v4.0.1 diag: role STILL unknown after the ensureRole attempt => the whole mod is idle (the
            //! HEARTBEAT below never runs). Log every ~10s so a stuck role (faulted probe / missing
            //! PalPlayerCharacter) is visible — the "reconnect -> mod dead" symptom.
            if (g_isSrv < 0) { static uint64_t lastUnknownLog = 0; if (now - lastUnknownLog > 10000) { lastUnknownLog = now; Output::send(STR("[ISGATE] ROLE still unknown (g_isSrv=-1) after ensureRole -> mod idle\n")); } }
            return;
        }
        //! Bring up the external TCP channel once the role is known (it can't start in on_unreal_init — the
        //! role needs an in-game PalPlayerCharacter). Authority listens; remote client connects; host/SP none.
        if (!g_netStarted) { g_netStarted = true; netStart(); }
        //! Periodic heartbeat: always-on (NOT gated by g_verbose or any log cap) so the mod's liveness and
        //! state are visible even after hundreds of CH log lines. Fires every ~30s.
        static uint64_t g_lastHeartbeat = 0;
        if (now - g_lastHeartbeat > 30000) {
            g_lastHeartbeat = now;
            const wchar_t* role = g_isSrv == 0 ? L"CLIENT" : (g_isDedi == 1 ? L"DEDI" : L"HOST");
            uint64_t sinceFetch = g_lastFetchOk ? (unsigned)(now - g_lastFetchOk) : 0;
            Output::send(STR("[ISGATE] HEARTBEAT role={} pool={} calls={} awaiting={} miss={} inCamp={} sinceFetch={}ms\n"),
                role, (int)g_pool.size(), (unsigned)g_myCalls, (int)g_awaitingReply, g_consecMiss, (int)g_inCampStable, (unsigned)sinceFetch);
        }
        //! CLIENT: pump the external TCP channel from the top-level tick. A request carrying the local camp
        //! GUID is queued to the net thread; replies that arrived over TCP are consumed here into g_pool.
        if (g_isSrv == 0) {
            //! consume any reply the net thread parked
            { std::wstring rep; { std::lock_guard<std::mutex> lk(g_netMu); if (g_cliHasReply) { rep = g_cliReply; g_cliHasReply = false; } } if (!rep.empty()) parsePoolReply(rep); }
            //! back-pressure: at most one request per CH_MIN_INTERVAL_MS, never while a reply is still
            //! outstanding (TCP is ordered, so an outstanding reply is the natural throttle signal).
            if (g_awaitingReply && (now - g_lastTrigAt) > CH_REPLY_TIMEOUT_MS) {
                g_awaitingReply = false;
                ++g_consecMiss;                                   // reply timed out -> channel unhealthy
                if (g_consecMiss >= 3 && g_missLogged < 3) {
                    ++g_missLogged;
                    Output::send(STR("[ISGATE] CH WARNING: {} consecutive replies missed ({}ms) — server not responding to this client's camp requests\n"),
                        g_consecMiss, (unsigned)(now - g_lastTrigAt));
                }
            }
            //! A2 — self-heal refresh: while CONFIRMED in camp, re-fetch the pool periodically (every ~12s)
            //! and on first entry. Recovers a pool wiped by a glitch / dropped reply without waiting for a
            //! re-enter or menu-open. Bounded by the back-pressure guard below.
            if (clientInCampStable() && (g_lastFetchOk == 0 || now - g_lastFetchOk > 12000)) g_needTrigger = true;
            if (g_needTrigger && !g_awaitingReply && (now - g_lastTrigAt) > CH_MIN_INTERVAL_MS) {
                std::wstring hex;
                if (chClientTrigger(hex)) {
                    g_lastTrigAt = now; g_needTrigger = false; g_awaitingReply = true;
                    g_common = findCommonContainer(); g_donorCont = findDonorContainer();
                    { std::lock_guard<std::mutex> lk(g_netMu); g_cliReqHex = hex; g_cliHasReq = true; }   // net thread sends "ISREQ|"+hex
                } else if (clientInCampStable()) {
                    g_lastTrigAt = now;   // in camp but guid not ready yet -> back off this cycle, retry later
                }
            }
            return;
        }
        //! AUTHORITY: serve pending TCP client requests. The net thread parked each peer's camp GUID in
        //! NetPeer::reqCampHex; here we resolve it, build the (guild - own) pool with delta, and stash the
        //! reply for the net thread to flush. EVERY UObject read happens on this game thread; the net thread
        //! only shuttles whole lines. Held under g_netMu (brief — a few ms for a handful of peers).
        { std::lock_guard<std::mutex> lk(g_netMu);
          for (auto* p : g_peers) {
              if (p->dead || !p->hasReq) continue;
              std::wstring hex = p->reqCampHex; p->hasReq = false;
              uint8_t guid[16];
              if (!hexToGuid(hex, guid)) continue;
              UObject* camp = srvCampById(guid);
              if (!camp) { if (g_errLog < 64) { ++g_errLog; Output::send(STR("[ISGATE] CH req: camp not found guid={} (not loaded yet -> no reply; client retries)\n"), hex.c_str()); } continue; }
              //! v4.0.3: a camp change forces a FULL re-sync. srvBuildForCamp builds (guild - own camp), so
              //! the baseline differs per camp; a delta against the previous camp's snapshot corrupts the
              //! client pool (Wood showed 11.4w in camp B vs 6.9w real = camp A items bled in).
              if (hex != p->lastCampHex) { p->snap.wantFull = true; p->lastCampHex = hex; }
              srvBuildReply(&p->snap, camp, p->reply); p->hasReply = true;
          } }
        //! AUTHORITY: ~8s discovery reconcile (guild state + container cross-registration for consume).
        if (g_lastReconcile == 0 || now - g_lastReconcile >= g_reconcileMs) { g_lastReconcile = now; srvDiscoverReconcile(); }
    }
private:
    auto install(uintptr_t base, Target& t, uint64_t cb) -> void {
        Sig s = parseSig(t.sig); int cnt = 0;
        uintptr_t addr = scanSig(s, &cnt);
        if (cnt != 1) { Output::send(STR("[ISGATE] {} SIG {} — skipped\n"), t.name, cnt == 0 ? STR("NOT FOUND") : STR("AMBIGUOUS")); return; }
        t.addr = addr;
        t.det = new PLH::x64Detour((uint64_t)addr, cb, &t.tramp);
        t.hooked = t.det->hook();
        Output::send(STR("[ISGATE] {} @ {:#x} (rva {:#x}) hooked={}\n"), t.name, addr, addr - base, t.hooked);
    }
};

#define IS_CPP_API __declspec(dllexport)
extern "C"
{
    IS_CPP_API CppUserModBase* start_mod() { return new ModIntegratedStorageCpp(); }
    IS_CPP_API void uninstall_mod(CppUserModBase* mod) { delete mod; }
}
