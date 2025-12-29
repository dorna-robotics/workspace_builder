import os, json, time
import re, ast, sys, importlib.util
import weakref
import tornado.web
import tornado.ioloop
import socketio
from tornado import autoreload
import yaml
try:
    import numpy as np
except Exception:
    np = None

# --- Builder anchor capture (robust to dorna2 internal changes) ---
_SOLID_ANCHORS_WEAK = weakref.WeakKeyDictionary()
_SOLID_ANCHORS_BY_ID = {}  # fallback if solids are not weakref-able

def _cache_solid_anchors(obj, anchors):
    if not isinstance(anchors, dict) or not anchors:
        return
    try:
        _SOLID_ANCHORS_WEAK[obj] = anchors
        return
    except Exception:
        pass
    try:
        _SOLID_ANCHORS_BY_ID[id(obj)] = anchors
    except Exception:
        pass


# Builder must mirror simulation's component instantiation to obtain anchors and
# solids. Components are the source of truth.

# optional: pose conversion for internal solid local transforms
try:
    from dorna2.pose import T_to_xyzabc
except Exception:
    T_to_xyzabc = None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BASE_DIR)

WS_PKG_DIR = os.path.join(PARENT_DIR, "workspace")
COMPONENTS_DIR = os.path.join(WS_PKG_DIR, "components")
CAD_DIR = os.path.join(PARENT_DIR, "static", "CAD")
print(f"[builder] server.py loaded from: {__file__}")
print(f"[builder] sys.path[0:3]: {sys.path[0:3]}")
# Ensure project root is on sys.path so `import workspace` resolves to the package, not workspace/workspace.py
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)
# --- Builder-only stubs for newer Core imports ---
# Newer versions of Core may import top-level modules that aren't present in the builder repo
# (e.g. `from camera import Camera`, `from path_planning import Planner`).
# When users "copy over the builder folder" into a fresh workspace checkout, we must still
# be able to instantiate Core without requiring any new files outside /builder.
from types import ModuleType

def _ensure_builder_stubs():
    # camera.py stub
    if "camera" not in sys.modules:
        cam_mod = ModuleType("camera")
        class Camera:  # minimal interface used by core; safe no-op
            def __init__(self, *a, **k):
                pass
        cam_mod.Camera = Camera
        sys.modules["camera"] = cam_mod

    # path_planning.py stub
    if "path_planning" not in sys.modules:
        pp_mod = ModuleType("path_planning")
        class Planner:  # minimal interface used by core; safe no-op
            def __init__(self, *a, **k):
                pass
            def check_collision(self, *a, **k):
                return []
        pp_mod.Planner = Planner
        sys.modules["path_planning"] = pp_mod

_ensure_builder_stubs()


# Patch dorna2 early (BEFORE importing workspace.components, which auto-imports all modules).
# This ensures any `from dorna2 import Solid` inside components gets the patched Solid.
_ORIG_DORNA = None
_ORIG_SOLID = None

def _patch_dorna2_once():
    global _ORIG_DORNA, _ORIG_SOLID
    try:
        import dorna2
    except Exception:
        return

    if _ORIG_DORNA is None:
        _ORIG_DORNA = getattr(dorna2, "Dorna", None)
    if _ORIG_SOLID is None:
        _ORIG_SOLID = getattr(dorna2, "Solid", None)

    class _DummyDorna:
        def __init__(self, *a, **k):
            pass
        def connect(self, *a, **k):
            return False
        def joint(self, *a, **k):
            return [0.0] * 8

    if getattr(dorna2, "Dorna", None) is not _DummyDorna:
        try:
            dorna2.Dorna = _DummyDorna
        except Exception:
            pass

    orig_solid = _ORIG_SOLID
    if orig_solid is None:
        return

    # If Solid is a Python class and subclassing works, use a subclass shim.
    try:
        class _BuilderSolid(orig_solid):
            def __init__(self, *a, **k):
                passed = k.get("anchors", None)
                super().__init__(*a, **k)
                _cache_solid_anchors(self, passed)
                # Also mirror to a conventional attribute if allowed.
                try:
                    cur = getattr(self, "anchors", None)
                    if (not isinstance(cur, dict) or not cur) and isinstance(passed, dict):
                        setattr(self, "anchors", passed)
                except Exception:
                    pass

        dorna2.Solid = _BuilderSolid
    except Exception:
        # Fallback: Solid might be non-subclassable (C-extension). Wrap it.
        def _SolidFactory(*a, **k):
            passed = k.get("anchors", None)
            obj = orig_solid(*a, **k)
            _cache_solid_anchors(obj, passed)
            try:
                cur = getattr(obj, "anchors", None)
                if (not isinstance(cur, dict) or not cur) and isinstance(passed, dict):
                    setattr(obj, "anchors", passed)
            except Exception:
                pass
            return obj
        dorna2.Solid = _SolidFactory

_patch_dorna2_once()

# Now that dorna2 is patched, importing components is safe.
from workspace.components import factory as comp_factory


REGISTER_RE = re.compile(r'@register\([\'"]([^\'"]+)[\'"]\)')

def scan_registered_components():
    out = {}
    for root, _, files in os.walk(COMPONENTS_DIR):
        for fn in files:
            if not fn.endswith(".py") or fn.startswith("_"):
                continue
            fp = os.path.join(root, fn)
            try:
                src = open(fp, "r", encoding="utf-8", errors="ignore").read()
            except Exception:
                continue
            for mm in REGISTER_RE.finditer(src):
                out[mm.group(1)] = fp
    return out

def ast_extract_cfg_get_options(src: str):
    try:
        tree = ast.parse(src)
    except Exception:
        return []
    opts = {}

    def _store_opt(key: str, default):
        """Store option with inferred kind/default."""
        if not isinstance(key, str) or not key:
            return
        # Don't expose the internal simulation toggle in Builder UI.
        if key == "simulation":
            return

        # infer kind
        boolish = key.startswith(("has_", "enable_", "use_", "is_")) or key.endswith(("_enabled", "_enable"))
        if isinstance(default, bool) or boolish:
            kind = "bool"
            if not isinstance(default, bool):
                default = False
        elif isinstance(default, int):
            kind = "int"
        elif isinstance(default, float):
            kind = "float"
        elif isinstance(default, (list, dict)) or default is None:
            kind = "json"
        else:
            kind = "text"

        # prefer existing entry (cfg.get might provide a better default)
        if key not in opts:
            opts[key] = {"name": key, "kind": kind, "default": default}

    # 1) Extract cfg.get("key", default)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        if not isinstance(fn, ast.Attribute) or fn.attr != "get":
            continue
        if not node.args:
            continue
        k0 = node.args[0]
        if not isinstance(k0, ast.Constant) or not isinstance(k0.value, str):
            continue
        key = k0.value
        default = None
        if len(node.args) >= 2 and isinstance(node.args[1], ast.Constant):
            default = node.args[1].value
        _store_opt(key, default)

    # 2) Extract class DEFAULTS = dict(...) or DEFAULTS = {...}
    # Newer components (notably Core) use DEFAULTS + mergedeep.merge(prm, cfg)
    # instead of cfg.get(...). Builder still needs to surface has_* toggles.
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        # target named DEFAULTS
        if not any(isinstance(t, ast.Name) and t.id == "DEFAULTS" for t in node.targets):
            continue

        v = node.value
        # DEFAULTS = dict(a=1, b=True, ...)
        if isinstance(v, ast.Call) and isinstance(v.func, ast.Name) and v.func.id == "dict":
            for kw in v.keywords or []:
                if not kw.arg:
                    continue
                if isinstance(kw.value, ast.Constant):
                    _store_opt(kw.arg, kw.value.value)
                else:
                    # non-constant default; still expose boolish toggles
                    _store_opt(kw.arg, None)
        # DEFAULTS = {"a": 1, "b": True}
        elif isinstance(v, ast.Dict):
            for kk, vv in zip(v.keys or [], v.values or []):
                if isinstance(kk, ast.Constant) and isinstance(kk.value, str):
                    key = kk.value
                    if isinstance(vv, ast.Constant):
                        _store_opt(key, vv.value)
                    else:
                        _store_opt(key, None)

    return list(opts.values())

def load_module_from_path(fp: str, unique_name: str):
    spec = importlib.util.spec_from_file_location(unique_name, fp)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def extract_anchors_from_instance(obj):
    anchors_by_solid = {}
    if hasattr(obj, "assembly") and isinstance(obj.assembly, dict):
        for solid_name, solid in obj.assembly.items():
            anchors = extract_solid_anchors(solid)
            if anchors:
                anchors_by_solid[solid_name] = anchors
    return anchors_by_solid


def _normalize_anchors(anchor_src):
    """Normalize anchors to a JSON-serializable dict[str, list[float]]."""
    if not isinstance(anchor_src, dict) or not anchor_src:
        return {}
    out = {}
    for k, v in anchor_src.items():
        if isinstance(v, (list, tuple)) and len(v) == 6:
            try:
                out[str(k)] = [float(v[i]) for i in range(6)]
            except Exception:
                continue
    return out


def extract_solid_anchors(solid):
    """Best-effort anchor extraction from a dorna2.Solid-like object.

    Priority (most reliable -> least):
      1) Anchors captured at Solid construction time by the Builder shim.
      2) Common attributes on Solid (anchors/anchor_dict/anchor).
      3) Common attributes on Solid.pose (if present).

    This is intentionally flexible so new components/new dorna2 versions keep working.
    """
    # 1) Builder-captured anchors (works even if dorna2 does not store them)
    try:
        val = _SOLID_ANCHORS_WEAK.get(solid)
    except Exception:
        val = None
    if val is None:
        try:
            val = _SOLID_ANCHORS_BY_ID.get(id(solid))
        except Exception:
            val = None
    norm = _normalize_anchors(val)
    if norm:
        return norm

    # 2) Captured by attribute (some shims may set this)
    for attr in ("_builder_anchors", "anchors", "anchor_dict", "anchor"):
        try:
            val = getattr(solid, attr, None)
        except Exception:
            val = None
        norm = _normalize_anchors(val)
        if norm:
            return norm

    # 3) Some implementations nest anchors under pose
    try:
        pose = getattr(solid, "pose", None)
    except Exception:
        pose = None
    if pose is not None:
        for attr in ("anchors", "anchor_dict", "anchor"):
            try:
                val = getattr(pose, attr, None)
            except Exception:
                val = None
            norm = _normalize_anchors(val)
            if norm:
                return norm

    return {}

def extract_anchors_from_module_globals(mod):
    anchors_by_solid = {}
    for k, v in mod.__dict__.items():
        if k.endswith("_anchors") and isinstance(v, dict) and v:
            solid = k[:-8]
            anchors_by_solid[solid] = v
    return anchors_by_solid


def _patch_dorna_for_builder():
    """Prevent components like core from attempting real robot connections."""
    try:
        import dorna2
    except Exception:
        return None

    # We patch both Dorna (to prevent real connections) and Solid (to reliably
    # capture anchors passed in at construction time, regardless of dorna2
    # internal attribute naming).
    orig_dorna = getattr(dorna2, "Dorna", None)
    orig_solid = getattr(dorna2, "Solid", None)

    class _DummyDorna:
        def __init__(self, *a, **k):
            pass

        def connect(self, *a, **k):
            return False

        def joint(self, *a, **k):
            return [0.0] * 8

    if orig_dorna is not None:
        dorna2.Dorna = _DummyDorna

    # --- Solid anchor capture shim ---
    # Some dorna2 versions do not expose anchors on `solid.anchors` (or use a
    # different internal structure). Builder must *always* be able to recover
    # the anchors dictionary that components pass to Solid(, anchors=).
    if orig_solid is not None:
        class _BuilderSolid(orig_solid):
            def __init__(self, *a, **k):
                passed_anchors = k.get("anchors", None)
                super().__init__(*a, **k)
                # Persist the raw anchors as provided by the component.
                try:
                    self._builder_anchors = passed_anchors
                except Exception:
                    pass
                # Best-effort: if dorna2 doesn't keep them anywhere obvious,
                # also mirror to a conventional attribute.
                try:
                    cur = getattr(self, "anchors", None)
                    if (not isinstance(cur, dict) or not cur) and isinstance(passed_anchors, dict):
                        setattr(self, "anchors", passed_anchors)
                except Exception:
                    pass

        dorna2.Solid = _BuilderSolid

    # Some components do: `from dorna2 import Dorna` at import time.
    # Patch any already-imported modules that captured the symbol.
    for mname, mod in list(sys.modules.items()):
        if not mname or not mod:
            continue
        if mname.startswith("workspace.components") and hasattr(mod, "Dorna"):
            try:
                setattr(mod, "Dorna", _DummyDorna)
            except Exception:
                pass

        # Some components do: `from dorna2 import Solid` at import time.
        # Patch any already-imported modules that captured the symbol.
        if mname.startswith("workspace.components") and hasattr(mod, "Solid") and orig_solid is not None:
            try:
                setattr(mod, "Solid", dorna2.Solid)
            except Exception:
                pass

    return (orig_dorna, orig_solid)


def _unpatch_dorna(orig):
    try:
        import dorna2
    except Exception:
        return
    orig_dorna, orig_solid = (orig or (None, None))
    if orig_dorna is not None:
        dorna2.Dorna = orig_dorna
    if orig_solid is not None:
        dorna2.Solid = orig_solid

    for mname, mod in list(sys.modules.items()):
        if not mname or not mod:
            continue
        if mname.startswith("workspace.components") and hasattr(mod, "Dorna"):
            try:
                setattr(mod, "Dorna", orig_dorna)
            except Exception:
                pass

        if mname.startswith("workspace.components") and hasattr(mod, "Solid") and orig_solid is not None:
            try:
                setattr(mod, "Solid", orig_solid)
            except Exception:
                pass


def _compute_world_Ts_for_solids(solids: dict):
    """Compute world transforms for a set of dorna2.Solid objects.

    Mirrors Workspace.compute_world_poses(), but scoped to an in-memory dict of
    solids (single component instantiation).

    Returns: dict solid_name -> 4x4 numpy array
    """
    if np is None:
        return {}

    # Roots are solids with no parent_solid
    roots = []
    for s in solids.values():
        try:
            if s.parent.get("parent_solid") is None:
                roots.append(s)
        except Exception:
            roots.append(s)

    world = {}
    stack = [(r, np.eye(4)) for r in roots]

    while stack:
        node, T_parent = stack.pop()
        try:
            T_local = node.local.get("T")
        except Exception:
            T_local = None
        if T_local is None:
            T_local = np.eye(4)

        try:
            T_world = T_parent @ T_local
        except Exception:
            T_world = np.array(T_parent)
        world[getattr(node, "name", str(id(node)))] = T_world

        # children structure: dict[key] -> list[{"child_solid": Solid, }]
        try:
            for child_list in getattr(node, "children", {}).values():
                for entry in child_list:
                    ch = entry.get("child_solid")
                    if ch is None:
                        continue
                    stack.append((ch, T_world))
        except Exception:
            pass

    return world


def instantiate_component_blueprint(type_name: str, options: dict):
    """
    Create the component exactly like simulation does (factory.create_component)
    and return a blueprint that the Builder UI can render.

    Returns:
      {
        "solids": [
          {
            "solid": "tool_rack",
            "glb": "/static/CAD/tool_rack.glb",
            "pose": [x,y,z,a,b,c],
            "anchors": {}
          },
          
        ]
      }
    """
    cfg = {"type": type_name}
    if isinstance(options, dict):
        cfg.update(options)

    dummy_ws = type("BuilderWS", (), {})()
    dummy_ws.components = {}
    # some components check this
    dummy_ws._scene_dirty = True



    # dorna2 is patched once at startup for Builder (see _patch_dorna2_once).
    comp = comp_factory.create_component(f"{type_name}_preview", cfg, dummy_ws)

    # If component exposes kinematic/attachment update logic (e.g., core),
    # ensure it is initialized in simulation mode so solids end up in the correct poses.
    try:
        if hasattr(comp, "update_pose"):
            # Some components (core) only update link poses when robot_api exists.
            if getattr(comp, "robot_api", None) is None:
                mod = sys.modules.get(comp.__class__.__module__)
                SimAPI = getattr(mod, "SimulationAPI", None) if mod else None
                if SimAPI is not None:
                    comp._simulation_mode = True
                    comp.robot_api = SimAPI(joints=[0.0] * 8)
            # run once to build attachment chain at zero joints
            comp.update_pose()
    except Exception:
        pass

    solids = []
    world_Ts = {}
    if hasattr(comp, "assembly") and isinstance(comp.assembly, dict):
        world_Ts = _compute_world_Ts_for_solids(comp.assembly)
        for solid_name, solid in comp.assembly.items():
            # solid.type is the CAD name
            stype = getattr(solid, "type", None) or solid_name
            glb_path = os.path.join(CAD_DIR, f"{stype}.glb")
            glb_url = f"/static/CAD/{stype}.glb" if os.path.exists(glb_path) else None

            pose = [0, 0, 0, 0, 0, 0]
            try:
                if T_to_xyzabc is not None:
                    # Prefer world pose (simulation-style). Fall back to local.
                    T = world_Ts.get(solid_name)
                    if T is None and hasattr(solid, "local") and isinstance(solid.local, dict):
                        T = solid.local.get("T")
                    if T is not None:
                        pose = T_to_xyzabc(T)
            except Exception:
                pass

            # Robust anchor extraction (works across dorna2 versions and our shim)
            anchors = extract_solid_anchors(solid)

            solids.append({
                "solid": solid_name,
                "solid_type": stype,
                "glb": glb_url,
                "pose": pose,
                "anchors": anchors,
            })

    return {"solids": solids}

COMPONENT_MAP = scan_registered_components()

STATIC_DIR = os.path.join(PARENT_DIR, "static")
WEB_DIR = os.path.join(BASE_DIR, "web")

OUT_DIR = os.path.join(PARENT_DIR, "projects", "builder")
OUT_PATH = os.path.join(OUT_DIR, "config.yaml")


class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('index.html')

PORT = int(os.environ.get("PORT", "5001"))
DEV_NOCACHE = os.environ.get("DEV_NOCACHE", "1") == "1"

sio = socketio.AsyncServer(async_mode="tornado", cors_allowed_origins="*")
app = tornado.web.Application([
    (r"/static/(.*)", tornado.web.StaticFileHandler, {"path": STATIC_DIR}),
    (r"/socket.io/(.*)", socketio.get_tornado_handler(sio)),
    # /save_config is added below after SaveConfigHandler definition
    (r"/", IndexHandler),
    (r"/(.*)", tornado.web.StaticFileHandler, {"path": WEB_DIR, "default_filename": "index.html"}),
], debug=True, template_path=WEB_DIR)


class CatalogHandler(tornado.web.RequestHandler):
    """Return list of registered component types (source of truth = workspace/components)."""
    def get(self):
        items = sorted(COMPONENT_MAP.keys())
        self.write({"ok": True, "items": items})

class TypeMetaHandler(tornado.web.RequestHandler):
    """Return anchors + options for a component type."""
    def get(self):
        t = self.get_argument("type", "")
        fp = COMPONENT_MAP.get(t)
        if not fp or not os.path.exists(fp):
            self.set_status(404)
            self.write({"ok": False, "error": f"unknown type: {t}"})
            return

        try:
            src = open(fp, "r", encoding="utf-8", errors="ignore").read()
        except Exception as e:
            self.set_status(500)
            self.write({"ok": False, "error": str(e)})
            return

        options = ast_extract_cfg_get_options(src)

        # Builder UI auto-generates checkboxes/fields from discovered options.
        # Hide internal toggles that exist in some components but are not useful in Builder.
        # IMPORTANT: do not hardcode has_* flags; only filter known-noisy ones.
        if "gripper" in str(t).lower():
            options = [o for o in options if o.get("name") != "output_enable"]

        if "decapper" in str(t).lower():
            options = [o for o in options if o.get("name") != "output_enable"]

        # Instantiate component with defaults (no options) to mirror simulation.
        anchors_by_solid = {}
        glb = None
        try:
            bp = instantiate_component_blueprint(t, {})
            solids = bp.get("solids") or []
            for s in solids:
                a = s.get("anchors")
                if isinstance(a, dict) and a:
                    anchors_by_solid[s.get("solid")] = a
            # choose a good preview glb
            for s in solids:
                if s.get("glb"):
                    glb = s.get("glb")
                    break
        except Exception:
            anchors_by_solid = {}

        self.write({"ok": True, "meta": {"type": t, "options": options, "anchors": anchors_by_solid, "glb": glb}})


class InstantiateHandler(tornado.web.RequestHandler):
    """Instantiate component (simulation-style) and return full solids blueprint."""
    def post(self):
        try:
            data = json.loads(self.request.body.decode("utf-8") or "{}")
        except Exception:
            self.set_status(400)
            self.write({"ok": False, "error": "invalid json"})
            return

        t = data.get("type") or ""
        if t not in COMPONENT_MAP:
            self.set_status(404)
            self.write({"ok": False, "error": f"unknown type: {t}"})
            return
        opts = data.get("options") or {}
        if not isinstance(opts, dict):
            opts = {}

        # Never allow builder UI/state to force non-simulation behavior.
        # Some components (notably Core) interpret this flag as "connect to real robot".
        opts.pop("simulation", None)

        try:
            bp = instantiate_component_blueprint(t, opts)
            self.write({"ok": True, "blueprint": bp})
        except Exception as e:
            self.set_status(500)
            self.write({"ok": False, "error": str(e)})

class SaveConfigHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "content-type")
        self.set_header("Access-Control-Allow-Methods", "POST, OPTIONS")

    def options(self):
        self.set_status(204)
        self.finish()

    def post(self):
        try:
            data = json.loads(self.request.body.decode("utf-8") or "{}")
        except Exception:
            self.set_status(400)
            self.finish({"ok": False, "error": "Invalid JSON"})
            return

        components = data.get("components") or {}
        if not isinstance(components, dict):
            self.set_status(400)
            self.finish({"ok": False, "error": "components must be an object"})
            return

        os.makedirs(OUT_DIR, exist_ok=True)

        # Write YAML similar to other project configs
        # Keep key order stable-ish: disable sort_keys
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            yaml.safe_dump(components, f, sort_keys=False, default_flow_style=False)

        self.finish({"ok": True, "path": OUT_PATH})

# patch handler into app
app.add_handlers(r".*$", [(r"/save_config", SaveConfigHandler)])

# catalog endpoint (CAD/*.glb)
app.add_handlers(r".*$", [(r"/api/catalog", CatalogHandler)])
app.add_handlers(r".*$", [(r"/api/type_meta", TypeMetaHandler)])
app.add_handlers(r".*$", [(r"/api/instantiate", InstantiateHandler)])

world_state = {}

def merge_into_state(state: dict, patch: dict):
    for k, v in (patch or {}).items():
        if v is None:
            continue
        if isinstance(v, dict) and v.get("delete"):
            state.pop(k, None)
        else:
            if k not in state:
                state[k] = {}
            if isinstance(v, dict):
                state[k].update(v)
            else:
                state[k] = v

@sio.event
async def connect(sid, environ, auth):
    if world_state:
        await sio.emit("scene_update", world_state, room=sid)

@sio.event
async def upstream_update(sid, payload):
    merge_into_state(world_state, payload)
    await sio.emit("scene_update", payload)
    return "ok"

if __name__ == "__main__":
    app.listen(PORT)
    print(f"[builder] listening at http://127.0.0.1:{PORT}")
    print(" - static:", STATIC_DIR)
    print(" - web:", WEB_DIR)
    print(" - save_config:", OUT_PATH)

    for p in (STATIC_DIR, WEB_DIR):
        if os.path.exists(p):
            autoreload.watch(p)
    autoreload.start()
    tornado.ioloop.IOLoop.current().start()