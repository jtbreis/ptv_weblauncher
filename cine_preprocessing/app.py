import os

from flask import Flask, jsonify, render_template, request

from preprocessing import (
    cine_frame_count,
    explorer_config,
    list_cameras,
    render_explorer_payload,
    resolve_run_dir,
)

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = os.environ.get('FLASK_TEMPLATES_AUTO_RELOAD', '1') == '1'

REPO_ROOT = os.environ.get(
    'REPO_ROOT',
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')),
)
JOB_LAUNCHER_URL = os.environ.get('JOB_LAUNCHER_URL', 'http://127.0.0.1:5000')


@app.route('/')
def index():
    return render_template(
        'index.html',
        repo_root=REPO_ROOT,
        job_launcher_url=JOB_LAUNCHER_URL,
    )


@app.route('/api/config')
def config_api():
    return jsonify(explorer_config(REPO_ROOT))


@app.route('/api/cameras')
def cameras_api():
    run_dir = request.args.get('run_dir', '').strip()
    if not run_dir:
        return jsonify({'error': 'run_dir required'}), 400
    try:
        resolved = resolve_run_dir(REPO_ROOT, run_dir)
        if not os.path.isdir(resolved):
            return jsonify({'error': f'not a directory: {resolved}'}), 404
        cameras = list_cameras(resolved)
        n_frames = 0
        if cameras:
            n_frames = cine_frame_count(os.path.join(resolved, f'{cameras[0]}.cine'))
        return jsonify({'cameras': cameras, 'n_frames': n_frames})
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@app.route('/api/render', methods=['POST'])
def render_api():
    body = request.get_json(silent=True) or {}
    try:
        payload = render_explorer_payload(
            REPO_ROOT,
            run_dir=body.get('run_dir', ''),
            camera=body.get('camera', 'Camera1'),
            frame_index=int(body.get('frame_index', 1)),
            center_window=int(body.get('center_window', 128)),
            display_vmax=float(body.get('display_vmax', 80)),
            pipeline_key=body.get('pipeline_key', 'percentile_bandpass'),
            params=body.get('params'),
            particle_diameter=int(body.get('particle_diameter', 7)),
            threshold=int(body.get('threshold', 10)),
            minmass=int(body.get('minmass', 0)),
        )
        return jsonify(payload)
    except FileNotFoundError as e:
        return jsonify({'error': str(e)}), 404
    except (ValueError, IndexError) as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', '5001'))
    app.run(host='0.0.0.0', port=port, threaded=True)
