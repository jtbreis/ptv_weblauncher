"""Discover PTV data runs and read HDF5 metadata for the web overview."""
from __future__ import annotations

import json
import os
import stat
import errno
from datetime import datetime, timezone
from typing import Any

import h5py
import numpy as np

# Pipeline artifact filenames (see src/python_4dptv/utils/structure.py and tracking scripts).
RAYS_H5 = "rays.h5"
RAYS_OUT_CPP_H5 = "rays_out_cpp.h5"
TRACKS_H5PART = "tracks.h5part"
TRACKS_STAGING_H5PART = "tracks_staging.h5part"
TRACKS_GRAPH_STAGING_H5PART = "tracks_graph_staging.h5part"
TRACKS_STAGING_JSON = "tracks_staging.json"
TRACKS_GRAPH_STAGING_JSON = "tracks_graph_staging.json"

WRITABLE_ARTIFACTS = frozenset({
    "rays_h5",
    "rays_out_cpp_h5",
    "tracks_staging_h5part",
    "tracks_h5part",
})

ARTIFACT_LABELS: dict[str, str] = {
    "rays_h5": "rays.h5",
    "rays_out_cpp_h5": "rays_out_cpp.h5",
    "tracks_staging_h5part": "tracks staging",
    "tracks_h5part": "tracks.h5part",
}

NON_RUN_DIRS = frozenset({"calibration", "calibration_after", "calibration_before"})

DEFAULT_REL_BASE = "Windtunnel_PTV_TNTI/X300cm_PTV_above_TNTI"

# Host mount points when data is not under REPO_ROOT (see jobs.yaml / devcontainer.json).
HOST_PROCESSED_MOUNT = "/mnt/raid1"
HOST_DATA_MOUNT = "/mnt/ssd"


def _attr_to_python(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if hasattr(value, "shape"):
        if getattr(value, "size", 0) == 0:
            return None
        if value.size == 1:
            return value.item()
        return value.flatten().tolist()
    return value


def _attr_to_str(value: Any) -> str:
    py = _attr_to_python(value)
    if py is None:
        return ""
    if isinstance(py, list):
        return ",".join(str(v) for v in py)
    return str(py)


def _frame_index(key: str) -> int:
    if key.startswith("frame"):
        try:
            return int(key[5:])
        except ValueError:
            pass
    return -1


def _step_index(key: str) -> int:
    if key.startswith("Step#"):
        try:
            return int(key[5:])
        except ValueError:
            pass
    return -1


def _scalar_attr(f: h5py.File, key: str) -> Any:
    if key not in f.attrs:
        return None
    return _attr_to_python(f.attrs[key])


def _summarize_rays_out(f: h5py.File) -> dict[str, Any]:
    frame_keys = sorted(
        (k for k in f.keys() if k.startswith("frame")),
        key=_frame_index,
    )
    match_counts: list[int] = []
    datasets: set[str] = set()
    for key in frame_keys:
        grp = f[key]
        if "xyze" in grp:
            sh = grp["xyze"].shape
            match_counts.append(int(sh[1] if len(sh) > 1 else sh[0]))
        else:
            match_counts.append(0)
        for name, item in grp.items():
            if isinstance(item, h5py.Dataset):
                datasets.add(name)

    nx, ny, nz = _scalar_attr(f, "nx"), _scalar_attr(f, "ny"), _scalar_attr(f, "nz")
    box = f"{nx}×{ny}×{nz}" if None not in (nx, ny, nz) else None

    summary: dict[str, Any] = {
        "file_kind": "rays_out_cpp",
        "stm_mincameras": _scalar_attr(f, "mincameras"),
        "stm_maxdistance": _scalar_attr(f, "maxdistance"),
        "stm_maxmatchesperray": _scalar_attr(f, "maxmatchesperray"),
        "stm_box_voxels": box,
        "matches_per_frame_min": min(match_counts) if match_counts else None,
        "matches_per_frame_max": max(match_counts) if match_counts else None,
        "matches_per_frame_mean": round(sum(match_counts) / len(match_counts), 1) if match_counts else None,
        "matches_total": sum(match_counts) if match_counts else None,
        "frame_datasets": sorted(datasets),
    }
    for src, dst in (
        ("parameters_multiple_matches_per_ray_distance", "stm_multiple_matches_per_ray_distance"),
        ("parameters_bounding_box", "stm_bounding_box"),
        ("parameters_spatial_boxes", "stm_spatial_boxes"),
        ("parameters_spatial_overlap_cells", "stm_spatial_overlap_cells"),
        ("parameters_frame_parallelism", "stm_frame_parallelism"),
        ("parameters_threads", "stm_threads"),
        ("parameters_timing", "stm_timing"),
        ("parameters_max_frames", "stm_max_frames_requested"),
        ("case", "run_case"),
        ("run", "run_name"),
        ("job_id", "job_id"),
    ):
        if src in f.attrs:
            summary[dst] = _attr_to_python(f.attrs[src])
    return summary


def _summarize_tracks(f: h5py.File) -> dict[str, Any]:
    step_keys = sorted(
        (k for k in f.keys() if k.startswith("Step#")),
        key=_step_index,
    )
    particle_counts: list[int] = []
    times: list[float] = []
    tracked_counts: list[int] = []
    unique_ids: set[int] = set()
    datasets: set[str] = set()

    for key in step_keys:
        grp = f[key]
        if "id" in grp:
            n = int(grp["id"].shape[0])
        elif "x" in grp:
            n = int(grp["x"].shape[0])
        else:
            n = 0
        particle_counts.append(n)
        if "Time" in grp.attrs:
            times.append(float(_attr_to_python(grp.attrs["Time"])))
        if "id" in grp:
            ids = np.asarray(grp["id"], dtype=np.int64).ravel()
            if ids.size:
                pos = ids[ids > 0]
                if pos.size:
                    unique_ids.update(int(x) for x in np.unique(pos))
                tracked_counts.append(int(pos.size))
        if not datasets:
            for name, item in grp.items():
                if isinstance(item, h5py.Dataset):
                    datasets.add(name)

    root_params = {
        k: _attr_to_python(f.attrs[k])
        for k in (
            "dt_s", "rep_rate_hz", "track_length", "position_units",
            "position_scale_to_m", "time_units", "velocity_units", "acceleration_units",
            "tracker", "job_id", "case", "run",
        )
        if k in f.attrs
    }
    for key in f.attrs:
        if key.startswith("parameters_"):
            root_params[key[len("parameters_"):]] = _attr_to_python(f.attrs[key])
    if step_keys:
        sample = f[step_keys[0]]
        for k in ("position_units", "time_units"):
            if k in sample.attrs and k not in root_params:
                root_params[k] = _attr_to_python(sample.attrs[k])

    summary: dict[str, Any] = {
        "file_kind": "tracks",
        "particles_per_step_min": min(particle_counts) if particle_counts else None,
        "particles_per_step_max": max(particle_counts) if particle_counts else None,
        "particles_per_step_mean": round(sum(particle_counts) / len(particle_counts), 1) if particle_counts else None,
        "time_min_s": min(times) if times else None,
        "time_max_s": max(times) if times else None,
        "n_unique_track_ids": len(unique_ids) if unique_ids else None,
        "step_datasets": sorted(datasets),
    }
    summary.update(root_params)
    if "case" in summary:
        summary["run_case"] = summary["case"]
    if "run" in summary:
        summary["run_name"] = summary["run"]
    if tracked_counts:
        summary["tracked_particles_per_step_mean"] = round(sum(tracked_counts) / len(tracked_counts), 1)
    return summary


def _detect_h5_kind(f: h5py.File, filepath: str) -> str | None:
    name = os.path.basename(filepath)
    if name == RAYS_OUT_CPP_H5 or (
        any(k.startswith("frame") for k in f.keys()) and "maxdistance" in f.attrs
    ):
        return "rays_out_cpp"
    if name.endswith(".h5part") or any(k.startswith("Step#") for k in f.keys()):
        return "tracks"
    if any(k.startswith("Camera") for k in f.keys()):
        return "rays"
    return None


def _summarize_h5_file(f: h5py.File, filepath: str) -> dict[str, Any] | None:
    kind = _detect_h5_kind(f, filepath)
    if kind == "rays_out_cpp":
        return _summarize_rays_out(f)
    if kind == "tracks":
        return _summarize_tracks(f)
    return None


def _count_h5_frames_from_file(f: h5py.File) -> int | None:
    """Count frames/steps in an open rays / rays_out / tracks HDF5 file."""
    frame_keys = [k for k in f.keys() if k.startswith("frame")]
    if frame_keys:
        return len(frame_keys)
    step_keys = [k for k in f.keys() if k.startswith("Step#")]
    if step_keys:
        return len(step_keys)
    for key in f.keys():
        if key.startswith("Camera"):
            cam = f[key]
            cam_frames = [k for k in cam.keys() if k.startswith("frame")]
            if cam_frames:
                return len(cam_frames)
    if "maxframes" in f.attrs:
        return int(f.attrs["maxframes"])
    return None


def _count_h5_frames(filepath: str) -> int | None:
    """Count frames/steps in rays.h5, rays_out_cpp.h5, or tracks*.h5part."""
    try:
        with h5py.File(filepath, "r") as f:
            return _count_h5_frames_from_file(f)
    except OSError:
        return None
    except Exception:
        return None


def collect_h5_metadata(filepath: str) -> dict[str, Any]:
    """Return metadata summary for one HDF5 file, or an error dict."""
    info: dict[str, Any] = {"path": filepath}
    try:
        st = os.stat(filepath)
        info["size_bytes"] = st.st_size
        info["modified_at"] = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat()
    except OSError as e:
        info["error"] = str(e)
        return info

    try:
        with h5py.File(filepath, "r") as f:
            attrs: dict[str, str] = {}
            for key in f.attrs:
                val = f.attrs[key]
                try:
                    attrs[key] = _attr_to_str(val)
                except Exception as e:
                    attrs[key] = f"<error: {e}>"
            info["n_top_level_keys"] = len(f.keys())
            info["attrs"] = attrs
            n_frames = _count_h5_frames_from_file(f)
            if n_frames is not None:
                info["n_frames"] = n_frames
            summary = _summarize_h5_file(f, filepath)
            if summary:
                info["summary"] = summary
    except Exception as e:
        info["error"] = str(e)
    return info


def normalize_base_path(base: str, *, root_kind: str) -> str:
    """Strip leading processed_data/ or data/ from a jobs.yaml-style base path."""
    base = (base or "").strip().replace("\\", "/").strip("/")
    for prefix in ("processed_data/", "data/"):
        if base.startswith(prefix):
            base = base[len(prefix) :]
    return base


def _first_existing_dir(*candidates: str) -> str | None:
    for path in candidates:
        if path and os.path.isdir(path):
            return os.path.normpath(path)
    return None


def resolve_roots(repo_root: str) -> tuple[str, str]:
    """Resolve processed_data and data roots.

    Layout (see jobs.yaml):
      processed_data (/mnt/raid1) — Centers_*, rays.h5
      data (/mnt/ssd) — rays_out_cpp.h5, tracks*.h5part
    """
    processed = os.environ.get("PROCESSED_DATA_ROOT")
    if not processed:
        processed = _first_existing_dir(
            os.path.join(repo_root, "processed_data"),
            HOST_PROCESSED_MOUNT,
        ) or os.path.join(repo_root, "processed_data")

    data = os.environ.get("DATA_ROOT")
    if not data:
        data = _first_existing_dir(
            os.path.join(repo_root, "data"),
            HOST_DATA_MOUNT,
        ) or os.path.join(repo_root, "data")

    return os.path.normpath(processed), os.path.normpath(data)


def case_dir(processed_root: str, data_root: str, base: str, case: str) -> tuple[str, str]:
    rel = normalize_base_path(base, root_kind="processed")
    return (
        os.path.join(processed_root, rel, case),
        os.path.join(data_root, rel, case),
    )


def list_cases(
    processed_root: str,
    data_root: str,
    base: str,
) -> list[str]:
    rel = normalize_base_path(base, root_kind="processed")
    cases: set[str] = set()
    for root in (processed_root, data_root):
        path = os.path.join(root, rel)
        if not os.path.isdir(path):
            continue
        for name in os.listdir(path):
            full = os.path.join(path, name)
            if os.path.isdir(full) and name.lower() not in NON_RUN_DIRS and name != "Calibration":
                cases.add(name)
    return sorted(cases)


def _run_has_centers(run_path: str) -> bool:
    if not os.path.isdir(run_path):
        return False
    if os.path.isdir(os.path.join(run_path, "Centers")):
        return True
    try:
        entries = os.listdir(run_path)
    except OSError:
        return False
    return any(
        name.startswith("Centers_TH") and os.path.isdir(os.path.join(run_path, name))
        for name in entries
    ) or os.path.isdir(os.path.join(run_path, "CenterData"))


def _is_run_dir(name: str, path: str) -> bool:
    if not os.path.isdir(path):
        return False
    if name.lower() in NON_RUN_DIRS or name == "Calibration":
        return False
    if name.lower().startswith("run"):
        return True
    return _run_has_centers(path) or any(
        os.path.isfile(os.path.join(path, fn))
        for fn in (RAYS_H5, RAYS_OUT_CPP_H5, TRACKS_H5PART, TRACKS_STAGING_H5PART)
    )


def discover_run_names(processed_case_dir: str, data_case_dir: str) -> list[str]:
    names: set[str] = set()
    for case_dir_path in (processed_case_dir, data_case_dir):
        if not os.path.isdir(case_dir_path):
            continue
        for name in os.listdir(case_dir_path):
            path = os.path.join(case_dir_path, name)
            if _is_run_dir(name, path):
                names.add(name)
    return sorted(names, key=lambda n: (not n.lower().startswith("run"), n))


def _read_json_sidecar(path: str) -> dict[str, Any] | None:
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def is_write_protected(path: str) -> bool | None:
    """Return True if no write bits are set; None if the path is missing."""
    try:
        mode = os.stat(path).st_mode
    except OSError:
        return None
    return (mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)) == 0


def _chmod_error_message(exc: OSError, path: str) -> str:
    if exc.errno == errno.EROFS:
        return (
            f"Read-only file system ({path}): data mounts must be read-write in the "
            "job-launcher container (remove :ro from /mnt/ssd and /mnt/raid1 in "
            "docker-compose.yml, then docker compose up -d --force-recreate)."
        )
    if exc.errno == errno.EACCES:
        return f"Permission denied ({path}): the launcher user cannot change file mode."
    return str(exc)


def set_write_protect(path: str, protect: bool) -> bool:
    """Remove or restore write permission on a file. Returns new write_protected state."""
    try:
        st = os.stat(path)
        if protect:
            new_mode = st.st_mode & ~0o222
        else:
            new_mode = (st.st_mode & ~0o777) | 0o644
        os.chmod(path, new_mode)
    except OSError as e:
        raise OSError(_chmod_error_message(e, path)) from e
    protected = is_write_protected(path)
    if protected is None:
        raise OSError(f"Could not stat file after chmod: {path}")
    return protected


def _path_under_root(path: str, root: str) -> bool:
    real_path = os.path.realpath(path)
    real_root = os.path.realpath(root)
    return real_path == real_root or real_path.startswith(real_root + os.sep)


def _artifact_path_for_run(processed_run_dir: str, data_run_dir: str, artifact: str) -> str | None:
    if artifact == "rays_h5":
        path = os.path.join(processed_run_dir, RAYS_H5)
    elif artifact == "rays_out_cpp_h5":
        path = os.path.join(data_run_dir, RAYS_OUT_CPP_H5)
    elif artifact == "tracks_staging_h5part":
        staging_name = _pick_staging_h5part(data_run_dir)
        path = os.path.join(data_run_dir, staging_name) if staging_name else ""
    elif artifact == "tracks_h5part":
        path = os.path.join(data_run_dir, TRACKS_H5PART)
    else:
        return None
    return path if path and os.path.isfile(path) else None


def resolve_artifact_paths(
    *,
    repo_root: str,
    base: str,
    case: str,
    artifact: str,
    run: str | None = None,
) -> list[str]:
    """Resolve on-disk paths for one pipeline artifact under a case (optionally one run)."""
    if artifact not in WRITABLE_ARTIFACTS:
        raise ValueError(f"Unknown artifact: {artifact}")
    processed_root, data_root = resolve_roots(repo_root)
    processed_case_dir, data_case_dir = case_dir(processed_root, data_root, base, case)
    run_names = [run] if run else discover_run_names(processed_case_dir, data_case_dir)
    paths: list[str] = []
    for name in run_names:
        pr = os.path.join(processed_case_dir, name)
        dr = os.path.join(data_case_dir, name)
        path = _artifact_path_for_run(pr, dr, artifact)
        if not path:
            continue
        if not (_path_under_root(path, processed_root) or _path_under_root(path, data_root)):
            raise ValueError(f"Path outside data roots: {path}")
        paths.append(path)
    return paths


def apply_write_protect(
    *,
    repo_root: str,
    base: str,
    case: str,
    artifact: str,
    protect: bool,
    run: str | None = None,
) -> dict[str, Any]:
    """Write-protect or unlock pipeline artifact files for a case or single run."""
    paths = resolve_artifact_paths(
        repo_root=repo_root, base=base, case=case, artifact=artifact, run=run,
    )
    if not paths:
        scope = f"run {run}" if run else "case"
        return {
            "ok": False,
            "error": f"No {ARTIFACT_LABELS.get(artifact, artifact)} files found for {scope}",
            "artifact": artifact,
            "case": case,
            "run": run,
            "results": [],
        }

    results: list[dict[str, Any]] = []
    for path in paths:
        entry: dict[str, Any] = {"path": path}
        try:
            entry["write_protected"] = set_write_protect(path, protect)
            entry["ok"] = True
        except OSError as e:
            entry["ok"] = False
            entry["error"] = e.args[0] if e.args else str(e)
        results.append(entry)

    changed = sum(1 for r in results if r.get("ok"))
    failed = [r for r in results if not r.get("ok")]
    return {
        "ok": changed > 0 and not failed,
        "partial": changed > 0 and bool(failed),
        "artifact": artifact,
        "case": case,
        "run": run,
        "write_protect": protect,
        "changed": changed,
        "total": len(results),
        "results": results,
        "error": failed[0]["error"] if failed and changed == 0 else None,
    }


def _artifact_info(run_dir: str, filename: str, *, read_h5: bool) -> dict[str, Any]:
    path = os.path.join(run_dir, filename)
    if not os.path.isfile(path):
        return {"present": False, "filename": filename}
    out: dict[str, Any] = {"present": True, "filename": filename, "path": path}
    protected = is_write_protected(path)
    if protected is not None:
        out["write_protected"] = protected
    if read_h5 and (filename.endswith(".h5") or filename.endswith(".h5part")):
        out.update(collect_h5_metadata(path))
    else:
        try:
            st = os.stat(path)
            out["size_bytes"] = st.st_size
            out["modified_at"] = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat()
        except OSError as e:
            out["error"] = str(e)
    return out


def _pick_staging_h5part(data_run_dir: str) -> str | None:
    for name in (TRACKS_STAGING_H5PART, TRACKS_GRAPH_STAGING_H5PART):
        if os.path.isfile(os.path.join(data_run_dir, name)):
            return name
    return None


def _pick_staging_json(data_run_dir: str) -> dict[str, Any] | None:
    for name in (TRACKS_STAGING_JSON, TRACKS_GRAPH_STAGING_JSON):
        data = _read_json_sidecar(os.path.join(data_run_dir, name))
        if data is not None:
            return data
    return None


def _infer_stage(artifacts: dict[str, dict[str, Any]]) -> str:
    if artifacts.get("tracks_h5part", {}).get("present"):
        return "tracks"
    if artifacts.get("tracks_staging_h5part", {}).get("present"):
        return "tracks_staging"
    if artifacts.get("rays_out_cpp_h5", {}).get("present"):
        return "stm"
    if artifacts.get("rays_h5", {}).get("present"):
        return "rays"
    if artifacts.get("centers"):
        return "centers"
    return "empty"


def _scan_run(name: str, processed_run_dir: str, data_run_dir: str) -> dict[str, Any]:
    """Lightweight run scan — presence checks and frame counts for HDF5 artifacts."""
    rays_path = os.path.join(processed_run_dir, RAYS_H5)
    stm_path = os.path.join(data_run_dir, RAYS_OUT_CPP_H5)
    tracks_path = os.path.join(data_run_dir, TRACKS_H5PART)
    rays_present = os.path.isfile(rays_path)
    stm_present = os.path.isfile(stm_path)
    tracks_present = os.path.isfile(tracks_path)
    artifacts: dict[str, Any] = {
        "centers": _run_has_centers(processed_run_dir),
        "rays_h5": rays_present,
        "rays_out_cpp_h5": stm_present,
        "tracks_staging_h5part": _pick_staging_h5part(data_run_dir) is not None,
        "tracks_h5part": tracks_present,
    }
    rays_frames = _count_h5_frames(rays_path) if rays_present else None
    stm_frames = _count_h5_frames(stm_path) if stm_present else None
    tracks_frames = _count_h5_frames(tracks_path) if tracks_present else None
    stage = _infer_stage({
        "centers": artifacts["centers"],
        "rays_h5": {"present": artifacts["rays_h5"]},
        "rays_out_cpp_h5": {"present": artifacts["rays_out_cpp_h5"]},
        "tracks_staging_h5part": {"present": artifacts["tracks_staging_h5part"]},
        "tracks_h5part": {"present": artifacts["tracks_h5part"]},
    })
    return {
        "name": name,
        "stage": stage,
        "rays_frames": rays_frames,
        "stm_frames": stm_frames,
        "tracks_frames": tracks_frames,
        "artifacts": artifacts,
    }


def build_run_record(name: str, processed_run_dir: str, data_run_dir: str) -> dict[str, Any]:
    staging_name = _pick_staging_h5part(data_run_dir) if os.path.isdir(data_run_dir) else None
    artifacts: dict[str, Any] = {
        "centers": _run_has_centers(processed_run_dir),
        "rays_h5": _artifact_info(processed_run_dir, RAYS_H5, read_h5=True),
        "rays_out_cpp_h5": _artifact_info(data_run_dir, RAYS_OUT_CPP_H5, read_h5=True),
        "tracks_staging_h5part": (
            _artifact_info(data_run_dir, staging_name, read_h5=True)
            if staging_name
            else {"present": False, "filename": TRACKS_STAGING_H5PART}
        ),
        "tracks_h5part": _artifact_info(data_run_dir, TRACKS_H5PART, read_h5=True),
    }
    staging_json = _pick_staging_json(data_run_dir) if os.path.isdir(data_run_dir) else None
    stage = _infer_stage(artifacts)
    return {
        "name": name,
        "stage": stage,
        "processed_dir": processed_run_dir if os.path.isdir(processed_run_dir) else None,
        "data_dir": data_run_dir if os.path.isdir(data_run_dir) else None,
        "artifacts": artifacts,
        "staging_json": staging_json,
    }


def _artifact_present(artifacts: dict[str, Any], key: str) -> bool:
    v = artifacts.get(key)
    if isinstance(v, bool):
        return v
    if isinstance(v, dict):
        return bool(v.get("present"))
    return False


def _infer_case_stage(summary: dict[str, Any]) -> str:
    total = summary.get("total_runs", 0)
    if total == 0:
        return "empty"
    if summary.get("with_tracks") == total:
        return "tracks"
    if summary.get("with_tracks", 0) > 0:
        return "tracks_partial"
    if summary.get("with_tracks_staging") == total:
        return "tracks_staging"
    if summary.get("with_tracks_staging", 0) > 0:
        return "tracks_staging_partial"
    if summary.get("with_stm") == total:
        return "stm"
    if summary.get("with_stm", 0) > 0:
        return "stm_partial"
    if summary.get("with_rays") == total:
        return "rays"
    if summary.get("with_rays", 0) > 0:
        return "rays_partial"
    if summary.get("with_centers") == total:
        return "centers"
    if summary.get("with_centers", 0) > 0:
        return "centers_partial"
    return "empty"


def _frame_range(runs: list[dict[str, Any]], key: str) -> tuple[int | None, int | None]:
    counts = [r[key] for r in runs if r.get(key) is not None]
    if not counts:
        return None, None
    return min(counts), max(counts)


def _summarize_runs(runs: list[dict[str, Any]]) -> dict[str, Any]:
    rays_frames_min, rays_frames_max = _frame_range(runs, "rays_frames")
    stm_frames_min, stm_frames_max = _frame_range(runs, "stm_frames")
    tracks_frames_min, tracks_frames_max = _frame_range(runs, "tracks_frames")
    summary = {
        "total_runs": len(runs),
        "with_centers": sum(1 for r in runs if _artifact_present(r["artifacts"], "centers")),
        "with_rays": sum(1 for r in runs if _artifact_present(r["artifacts"], "rays_h5")),
        "with_stm": sum(1 for r in runs if _artifact_present(r["artifacts"], "rays_out_cpp_h5")),
        "with_tracks_staging": sum(1 for r in runs if _artifact_present(r["artifacts"], "tracks_staging_h5part")),
        "with_tracks": sum(1 for r in runs if _artifact_present(r["artifacts"], "tracks_h5part")),
        "rays_frames_min": rays_frames_min,
        "rays_frames_max": rays_frames_max,
        "stm_frames_min": stm_frames_min,
        "stm_frames_max": stm_frames_max,
        "tracks_frames_min": tracks_frames_min,
        "tracks_frames_max": tracks_frames_max,
    }
    summary["stage"] = _infer_case_stage(summary)
    return summary


def _build_case_status(
    case: str,
    processed_case_dir: str,
    data_case_dir: str,
) -> dict[str, Any]:
    run_names = discover_run_names(processed_case_dir, data_case_dir)
    runs = [
        _scan_run(
            name,
            os.path.join(processed_case_dir, name),
            os.path.join(data_case_dir, name),
        )
        for name in run_names
    ]
    return {
        "name": case,
        "summary": _summarize_runs(runs),
        "processed_case_dir": processed_case_dir if os.path.isdir(processed_case_dir) else None,
        "data_case_dir": data_case_dir if os.path.isdir(data_case_dir) else None,
    }


def _run_detail_view(run: dict[str, Any]) -> dict[str, Any]:
    """Per-run detail: full HDF5 metadata + tracking parameters from staging JSON."""
    arts = run.get("artifacts") or {}
    sj = run.get("staging_json") or {}

    tracking_params = sj.get("parameters")
    if isinstance(tracking_params, str):
        try:
            tracking_params = json.loads(tracking_params)
        except json.JSONDecodeError:
            pass

    qc = sj.get("quality_check") or {}
    qc_summary = {}
    if qc:
        for key in (
            "overall_ok", "counts_ok", "n_frames", "min_particles", "mean_particles",
            "max_particles", "total_tracks", "vz_count_ok", "normality_ok",
        ):
            if key in qc:
                qc_summary[key] = qc[key]
        if "thresholds" in qc:
            qc_summary["thresholds"] = qc["thresholds"]

    staging_sidecar = {}
    for key in (
        "timestamp_utc", "tracker", "workers", "n_frames", "num_frames", "n_blocks",
        "total_tracks", "elapsed_s", "duration_seconds", "input_path", "output_path",
    ):
        if key in sj:
            staging_sidecar[key] = sj[key]
    if isinstance(tracking_params, dict):
        staging_sidecar["parameters"] = tracking_params
    if qc_summary:
        staging_sidecar["quality_check"] = qc_summary

    return {
        "name": run.get("name"),
        "stage": run.get("stage"),
        "processed_dir": run.get("processed_dir"),
        "data_dir": run.get("data_dir"),
        "centers": bool(arts.get("centers")),
        "artifacts": arts,
        "staging_json": staging_sidecar or None,
    }


def build_case_detail(*, repo_root: str, case: str, base: str = DEFAULT_REL_BASE) -> dict[str, Any]:
    overview = build_case_overview(repo_root=repo_root, case=case, base=base)
    overview["runs"] = [_run_detail_view(r) for r in overview.get("runs") or []]
    overview["generated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return overview


def build_case_overview(
    *,
    repo_root: str,
    case: str,
    base: str = DEFAULT_REL_BASE,
) -> dict[str, Any]:
    processed_root, data_root = resolve_roots(repo_root)
    rel = normalize_base_path(base, root_kind="processed")
    processed_case_dir, data_case_dir = case_dir(processed_root, data_root, base, case)
    run_names = discover_run_names(processed_case_dir, data_case_dir)
    runs = []
    for name in run_names:
        pr = os.path.join(processed_case_dir, name)
        dr = os.path.join(data_case_dir, name)
        runs.append(build_run_record(name, pr, dr))

    summary = _summarize_runs(runs)

    return {
        "case": case,
        "base": rel,
        "processed_root": processed_root,
        "data_root": data_root,
        "processed_case_dir": processed_case_dir,
        "data_case_dir": data_case_dir,
        "processed_case_exists": os.path.isdir(processed_case_dir),
        "data_case_exists": os.path.isdir(data_case_dir),
        "summary": summary,
        "runs": runs,
    }


def build_base_overview(*, repo_root: str, base: str = DEFAULT_REL_BASE) -> dict[str, Any]:
    """Case-level pipeline status for all cases under one base path."""
    processed_root, data_root = resolve_roots(repo_root)
    rel = normalize_base_path(base, root_kind="processed")
    case_names = list_cases(processed_root, data_root, base)
    cases = []
    totals = {
        "total_cases": 0,
        "total_runs": 0,
        "with_centers": 0,
        "with_rays": 0,
        "with_stm": 0,
        "with_tracks_staging": 0,
        "with_tracks": 0,
    }
    for case in case_names:
        processed_case_dir, data_case_dir = case_dir(processed_root, data_root, base, case)
        entry = _build_case_status(case, processed_case_dir, data_case_dir)
        summary = entry["summary"]
        cases.append(entry)
        totals["total_cases"] += 1
        totals["total_runs"] += summary["total_runs"]
        for key in ("with_centers", "with_rays", "with_stm", "with_tracks_staging", "with_tracks"):
            totals[key] += summary[key]

    processed_base_dir = os.path.join(processed_root, rel)
    data_base_dir = os.path.join(data_root, rel)

    return {
        "base": rel,
        "processed_root": processed_root,
        "data_root": data_root,
        "processed_base_dir": processed_base_dir,
        "data_base_dir": data_base_dir,
        "processed_base_exists": os.path.isdir(processed_base_dir),
        "data_base_exists": os.path.isdir(data_base_dir),
        "summary": totals,
        "cases": cases,
    }


def default_config(repo_root: str) -> dict[str, Any]:
    processed_root, data_root = resolve_roots(repo_root)
    default_base = os.environ.get("DATA_DEFAULT_BASE", DEFAULT_REL_BASE)
    return {
        "processed_root": processed_root,
        "data_root": data_root,
        "processed_root_exists": os.path.isdir(processed_root),
        "data_root_exists": os.path.isdir(data_root),
        "default_base": default_base,
    }
