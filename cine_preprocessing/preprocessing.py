"""Image rendering and API payload assembly for the cine preprocessing app."""
from __future__ import annotations

import base64
import io
import os
import sys
from functools import lru_cache
from typing import Any

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np
from matplotlib.patches import Rectangle

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.environ.get(
    'REPO_ROOT',
    os.path.abspath(os.path.join(_APP_DIR, '..', '..')),
)
for _path in (_APP_DIR, os.path.join(_REPO_ROOT, 'scripts')):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from cine_preprocessing_core import (  # noqa: E402
    DEFAULT_PARAMS,
    PIPELINE_CHOICES,
    build_bandpass_then_bg_panels,
    build_bandpass_threshold_panels,
    build_gaussian_percentile_panels,
    build_stretch_background_panels,
    center_roi,
    crop_roi,
    discover_cine_files,
    discover_run_dirs,
    cine_frame_count,
    load_cine_frame,
    locate_particles,
    particles_in_roi,
    run_pipeline,
    shift_features_to_crop,
)

DEFAULT_RUN_DIR = (
    'raw_data/2025-09-11-ParticleTracking/TTI_aligned_with_gravity/Run4'
)


def default_run_dir(repo_root: str) -> str:
    return os.path.join(repo_root, DEFAULT_RUN_DIR)


def resolve_run_dir(repo_root: str, run_dir: str) -> str:
    run_dir = (run_dir or '').strip()
    if not run_dir:
        run_dir = default_run_dir(repo_root)
    if not os.path.isabs(run_dir):
        run_dir = os.path.join(repo_root, run_dir)
    return os.path.abspath(run_dir)


def list_runs(repo_root: str) -> list[dict[str, str]]:
    raw_root = os.path.join(repo_root, 'raw_data')
    runs = discover_run_dirs(raw_root)
    out = [{'path': path, 'label': os.path.relpath(path, repo_root)} for path in runs]
    default = default_run_dir(repo_root)
    if os.path.isdir(default) and not any(r['path'] == default for r in out):
        out.insert(0, {
            'path': default,
            'label': os.path.relpath(default, repo_root),
        })
    return out


def list_cameras(run_dir: str) -> list[str]:
    paths = discover_cine_files(run_dir)
    return [os.path.splitext(os.path.basename(p))[0] for p in paths]


@lru_cache(maxsize=8)
def _cached_frame(cine_path: str, frame_index: int) -> np.ndarray:
    return load_cine_frame(cine_path, frame_index)


def _fig_to_b64(fig: plt.Figure) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=110, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('ascii')


def _render_gray(
    img: np.ndarray,
    title: str,
    *,
    vmax: float | None = None,
    roi: list[int] | None = None,
    features=None,
    diameter: int = 7,
    figsize: tuple[float, float] = (4.2, 4.2),
) -> str:
    fig, ax = plt.subplots(figsize=figsize, dpi=110, facecolor='#141614')
    ax.set_facecolor('#141614')
    kwargs = {'cmap': 'gray', 'interpolation': 'nearest'}
    if vmax is not None:
        kwargs['vmin'] = 0
        kwargs['vmax'] = vmax
    ax.imshow(img, **kwargs)
    if roi is not None:
        x0, y0, w, h = roi
        ax.add_patch(Rectangle((x0, y0), w, h, fill=False, edgecolor='#00ff88', linewidth=1.2))
    if features is not None and len(features):
        ax.plot(
            features['x'],
            features['y'],
            'o',
            fillstyle='none',
            markeredgecolor='#00ff88',
            markersize=diameter * 0.55,
            markeredgewidth=0.7,
        )
    ax.set_title(title, fontsize=9, color='#d4d8d4')
    ax.axis('off')
    fig.tight_layout(pad=0.2)
    return _fig_to_b64(fig)


def _merge_params(overrides: dict[str, Any] | None) -> dict[str, Any]:
    params = dict(DEFAULT_PARAMS)
    if overrides:
        params.update(overrides)
    return params


def render_explorer_payload(
    repo_root: str,
    *,
    run_dir: str,
    camera: str,
    frame_index: int,
    center_window: int,
    display_vmax: float,
    pipeline_key: str,
    params: dict[str, Any] | None,
    particle_diameter: int,
    threshold: int,
    minmass: int,
) -> dict[str, Any]:
    run_dir = resolve_run_dir(repo_root, run_dir)
    repo_abs = os.path.abspath(repo_root)
    if not run_dir.startswith(repo_abs):
        raise ValueError('Run directory must be under repo root')

    cine_path = os.path.join(run_dir, f'{camera}.cine')
    if not os.path.isfile(cine_path):
        raise FileNotFoundError(f'Missing {cine_path}')

    merged = _merge_params(params)
    frame = _cached_frame(cine_path, int(frame_index))
    n_frames = cine_frame_count(cine_path)
    roi = center_roi(frame, int(center_window))
    pipeline = run_pipeline(frame, pipeline_key, merged)

    pipeline_images = [
        {
            'title': label,
            'image': _render_gray(
                crop_roi(img, roi),
                label,
                vmax=display_vmax if label == 'Raw' else None,
            ),
        }
        for label, img in pipeline.steps
    ]
    pipeline_images.append({
        'title': 'Full frame + center window',
        'image': _render_gray(
            frame,
            f'{camera} frame {frame_index}',
            vmax=display_vmax,
            roi=roi,
            figsize=(7.5, 4.5),
        ),
    })

    stretch_compare = [
        {
            'title': title,
            'image': _render_gray(
                crop_roi(img, roi),
                title,
                vmax=display_vmax if key == 'percentile' else None,
            ),
        }
        for key, title, img in build_stretch_background_panels(frame, merged)
    ]

    gauss_compare = [
        {
            'title': title,
            'image': _render_gray(
                crop_roi(img, roi),
                title,
                vmax=display_vmax if key in ('gaussian', 'percentile') else None,
            ),
        }
        for key, title, img in build_gaussian_percentile_panels(frame, merged)
    ]

    # Compare on center crop only — rolling-ball on full frames is too slow for the UI
    frame_roi = crop_roi(frame, roi)
    bandpass_compare = [
        {
            'title': title,
            'image': _render_gray(
                img,
                title,
                vmax=display_vmax if key == 'raw' else None,
            ),
        }
        for key, title, img in build_bandpass_then_bg_panels(frame_roi, merged)
    ]
    threshold_compare = [
        {
            'title': title,
            'image': _render_gray(
                img,
                title,
                vmax=display_vmax if key == 'raw' else None,
            ),
        }
        for key, title, img in build_bandpass_threshold_panels(frame_roi, merged)
    ]

    detect_on = pipeline.final if pipeline_key != 'raw' else frame
    preprocess = pipeline_key == 'raw'
    features_full = locate_particles(
        detect_on,
        diameter=int(particle_diameter),
        threshold=int(threshold),
        minmass=int(minmass),
        preprocess=preprocess,
    )
    features_roi = shift_features_to_crop(particles_in_roi(features_full, roi), roi)
    raw_view = crop_roi(frame, roi)
    proc_view = crop_roi(detect_on, roi)

    detection = {
        'count_full': int(len(features_full)),
        'count_roi': int(len(features_roi)),
        'raw_image': _render_gray(
            raw_view,
            f'Raw (n={len(features_roi)})',
            vmax=display_vmax,
            features=features_roi if len(features_roi) else None,
            diameter=int(particle_diameter),
        ),
        'processed_image': _render_gray(
            proc_view,
            f'Processed (n={len(features_roi)})',
            features=features_roi if len(features_roi) else None,
            diameter=int(particle_diameter),
        ),
        'particles': (
            features_roi[['x', 'y', 'mass', 'size', 'signal']]
            .sort_values('mass', ascending=False)
            .to_dict(orient='records')
            if len(features_roi) else []
        ),
    }

    return {
        'meta': {
            'camera': camera,
            'frame_index': int(frame_index),
            'n_frames': int(n_frames),
            'shape': list(frame.shape),
            'dtype': str(frame.dtype),
            'min': int(frame.min()),
            'max': int(frame.max()),
            'mean': float(frame.mean()),
            'roi': roi,
            'pipeline_title': pipeline.title,
        },
        'pipeline': pipeline_images,
        'compare_stretch': stretch_compare,
        'compare_gaussian': gauss_compare,
        'compare_bandpass': bandpass_compare,
        'compare_threshold': threshold_compare,
        'detection': detection,
    }


def explorer_config(repo_root: str) -> dict[str, Any]:
    return {
        'default_run_dir': default_run_dir(repo_root),
        'runs': list_runs(repo_root),
        'pipelines': [{'key': k, 'label': v} for k, v in PIPELINE_CHOICES.items()],
        'default_params': DEFAULT_PARAMS,
        'default_pipeline': 'bandpass_threshold',
    }
